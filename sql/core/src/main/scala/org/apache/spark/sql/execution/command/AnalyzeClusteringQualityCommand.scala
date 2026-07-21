/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.command

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

/**
 * The logical plan of `ANALYZE TABLE t COMPUTE CLUSTERING QUALITY`.
 *
 * A '''read-only''' probe (no commit, no property write) that reports how well a table is clustered
 * to its CURRENT key selection, using only what OPTIMIZE persists (`optimize.cluster.state`) plus
 * manifest metrics (`t.files`). Designed to back an operational SLA of the form "P% of data is
 * clustered to quality X within Y hours"; whether that yields data skipping is the customer's own
 * validation (query-pattern dependent, deliberately out of scope).
 *
 * All metrics are computed with '''distributed SQL over metadata''' (an aggregate for coverage, a
 * windowed sweep for depth) rather than collecting per-file rows to the driver, so the command is
 * safe on tables with very large file counts.
 *
 * Output rows: `(metric, dimension, value)` where `dimension` is set only for per-key depth rows.
 */
case class AnalyzeClusteringQualityCommand(nameParts: Seq[String]) extends LeafRunnableCommand {

  import AnalyzeClusteringQualityCommand._
  import OptimizeTableCommand.{KEYS_PROP, SORT_MODE_PROP, STATE_PROP, HWM_PROP,
    DEFAULT_SORT_MODE, configId, parseState}

  override lazy val output: Seq[Attribute] = Seq(
    AttributeReference("metric", StringType, nullable = false)(),
    AttributeReference("dimension", StringType, nullable = true)(),
    AttributeReference("value", StringType, nullable = false)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalogManager = sparkSession.sessionState.catalogManager
    val (catalog, table) = nameParts match {
      case head +: tail if tail.nonEmpty && catalogManager.isCatalogRegistered(head) =>
        (head, tail)
      case _ =>
        (catalogManager.currentCatalog.name, nameParts)
    }
    val cat = quoteIfNeeded(catalog)
    val tableArg = table.map(quoteIfNeeded).mkString(".")
    val qualifiedTableName = s"$cat.$tableArg"

    val props = catalogManager.catalog(catalog).asTableCatalog
      .loadTable(Identifier.of(table.init.toArray, table.last)).properties().asScala.toMap
    val keys = props.get(KEYS_PROP)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Seq.empty)

    val out = mutable.ArrayBuffer[Row]()

    if (keys.isEmpty) {
      out += Row("clustering_configured", null, "false")
      return out.toSeq
    }
    out += Row("clustering_configured", null, "true")

    val sortMode = props.getOrElse(SORT_MODE_PROP, DEFAULT_SORT_MODE)
    val cfgId = configId(keys, sortMode)
    out += Row("config_id", null, cfgId)
    out += Row("keys", null, keys.mkString(","))
    out += Row("sort_mode", null, sortMode)

    val leadKey = keys.head
    val leadType = sparkSession.table(qualifiedTableName).schema(leadKey).dataType.sql
    val current = parseState(props.getOrElse(STATE_PROP, "")).filter(_.config == cfgId)

    // Coverage: a file is covered iff its leading-key range fits inside a current-config interval.
    // Computed as one aggregate over manifest metrics -- no per-file collect.
    val leadLo = metricExpr(leadKey, "lower_bound")
    val leadHi = metricExpr(leadKey, "upper_bound")
    val coveredExpr = coveragePredicate(leadLo, leadHi, current, leadType)
    val a = sparkSession.sql(
      s"""SELECT count(*) AS files_total,
         |  coalesce(sum(file_size_in_bytes), 0) AS bytes_total,
         |  coalesce(sum(CASE WHEN cov THEN 1 ELSE 0 END), 0) AS files_covered,
         |  coalesce(sum(CASE WHEN cov THEN file_size_in_bytes ELSE 0 END), 0) AS bytes_covered,
         |  coalesce(sum(CASE WHEN lead_null THEN file_size_in_bytes ELSE 0 END), 0) AS null_bytes
         |FROM (SELECT file_size_in_bytes,
         |        coalesce($coveredExpr, false) AS cov,
         |        ($leadLo IS NULL OR $leadHi IS NULL) AS lead_null
         |      FROM $qualifiedTableName.files)""".stripMargin).collect().head
    val filesTotal = a.getLong(0)
    val bytesTotal = a.getLong(1)
    val filesCovered = a.getLong(2)
    val bytesCovered = a.getLong(3)
    val nullBytes = a.getLong(4)
    out += Row("files_total", null, filesTotal.toString)
    out += Row("files_covered", null, filesCovered.toString)
    out += Row("bytes_total", null, bytesTotal.toString)
    out += Row("bytes_covered", null, bytesCovered.toString)
    out += Row("coverage_bytes_pct", null, pct(bytesCovered, bytesTotal))
    out += Row("coverage_files_pct", null, pct(filesCovered, filesTotal))
    out += Row("null_bound_bytes_pct", null, pct(nullBytes, bytesTotal))

    // Depth per clustering dimension: global and over the covered region only (the SLA input).
    // Each is a windowed stabbing-count sweep over metadata, kept off the driver.
    keys.foreach { k =>
      val kLo = metricExpr(k, "lower_bound")
      val kHi = metricExpr(k, "upper_bound")
      val g = depthStats(sparkSession, qualifiedTableName, kLo, kHi, None)
      val c = depthStats(sparkSession, qualifiedTableName, kLo, kHi, Some(coveredExpr))
      out += Row("depth_avg", k, fmt(g.avg))
      out += Row("depth_p90", k, fmt(g.p90))
      out += Row("depth_max", k, g.max.toString)
      out += Row("depth_avg_covered", k, fmt(c.avg))
      out += Row("depth_p90_covered", k, fmt(c.p90))
    }

    val tail = tailHours(sparkSession, qualifiedTableName, props.get(HWM_PROP))
    out += Row("unclustered_tail_hours", null, tail)
    out += Row("state", null, props.getOrElse(STATE_PROP, "[]"))
    out.toSeq
  }
}

object AnalyzeClusteringQualityCommand {

  final case class DepthStats(avg: Double, p90: Double, max: Long)

  /** SQL access to a per-file column metric, e.g. `readable_metrics.`ts`.lower_bound`. */
  def metricExpr(key: String, field: String): String =
    s"readable_metrics.${quoteIfNeeded(key)}.$field"

  /**
   * SQL boolean: the leading-key range `[lo, hi]` fits inside some current-config interval
   * `(lower, upper]`. Interval bounds are CAST to the leading-key type; the value is embedded as a
   * Catalyst literal so quotes survive. `null` bounds make the expression `null` (-> uncovered via
   * the caller's `coalesce(..., false)`). Exposed for testing.
   */
  def coveragePredicate(
      loExpr: String,
      hiExpr: String,
      intervals: Seq[OptimizeTableCommand.ClusterInterval],
      castType: String): String = {
    if (intervals.isEmpty) return "false"
    intervals.map { iv =>
      val upper = s"($hiExpr <= CAST(${Literal(iv.upper).sql} AS $castType))"
      val lower = iv.lower match {
        case Some(lo) => s"($loExpr > CAST(${Literal(lo).sql} AS $castType))"
        case None => "true"
      }
      s"($upper AND $lower)"
    }.mkString(" OR ")
  }

  private def pct(part: Long, total: Long): String =
    if (total == 0) "0.0" else fmt(100.0 * part / total)

  private def fmt(d: Double): String = f"$d%.2f"

  /**
   * Stabbing-depth stats over the `[lower, upper]` intervals of one dimension, optionally
   * restricted to the covered region. Computed with a windowed running-sum sweep in SQL (`+1` at
   * each lower bound, `-1` past each upper, sampled at start events) so nothing is collected to the
   * driver. Depth `1` means no overlap (perfectly clustered); higher means more interleaving.
   */
  private def depthStats(
      spark: SparkSession,
      qualifiedTableName: String,
      loExpr: String,
      hiExpr: String,
      coveredFilter: Option[String]): DepthStats = {
    val extra = coveredFilter.map(f => s"AND coalesce($f, false)").getOrElse("")
    val where = s"$loExpr IS NOT NULL AND $hiExpr IS NOT NULL $extra"
    val q =
      s"""WITH ev AS (
         |  SELECT $loExpr AS pt, 1 AS delta FROM $qualifiedTableName.files WHERE $where
         |  UNION ALL
         |  SELECT $hiExpr AS pt, -1 AS delta FROM $qualifiedTableName.files WHERE $where
         |),
         |running AS (SELECT delta, sum(delta) OVER (ORDER BY pt, delta DESC) AS depth FROM ev)
         |SELECT coalesce(avg(CASE WHEN delta = 1 THEN CAST(depth AS DOUBLE) END), 0.0),
         |  coalesce(percentile_approx(
         |    CASE WHEN delta = 1 THEN CAST(depth AS DOUBLE) END, 0.9), 0.0),
         |  coalesce(max(depth), 0L)
         |FROM running""".stripMargin
    val r = spark.sql(q).collect().head
    DepthStats(r.getDouble(0), r.getDouble(1), r.getLong(2))
  }

  /**
   * Age in hours of the oldest not-yet-clustered data: the oldest non-replace snapshot committed
   * after the watermark snapshot. `0` if nothing is newer than the watermark; `unknown` if the
   * watermark is unset or has been expired (so an SLA breach is never hidden).
   */
  private def tailHours(
      spark: SparkSession, qualifiedTableName: String, hwm: Option[String]): String = {
    hwm match {
      case None => "unknown"
      case Some(h) =>
        val floor = spark.table(s"$qualifiedTableName.snapshots")
          .where(col("snapshot_id") === h.toLong).select("committed_at").collect()
        if (floor.isEmpty) return "unknown" // expired watermark
        val rows = spark.sql(
          s"""SELECT CAST((unix_timestamp(current_timestamp()) -
             |  unix_timestamp(min(committed_at))) / 3600.0 AS DOUBLE)
             |FROM $qualifiedTableName.snapshots
             |WHERE operation != 'replace'
             |  AND committed_at > (SELECT committed_at FROM $qualifiedTableName.snapshots
             |    WHERE snapshot_id = $h)""".stripMargin).collect()
        if (rows.isEmpty || rows.head.isNullAt(0)) "0.0" else fmt(rows.head.getDouble(0))
    }
  }
}
