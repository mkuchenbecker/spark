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

import scala.collection.mutable

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.types.StringType

/**
 * The logical plan of `ANALYZE TABLE t COMPUTE CLUSTERING QUALITY`.
 *
 * A '''read-only''' probe (no commit, no property write) that reports how well a table is clustered
 * to its CURRENT key selection, using only what OPTIMIZE persists (`optimize.cluster.state`)
 * plus manifest metrics (`t.files`). Designed to back an operational SLA of the form
 * "P% of data is clustered to quality X within Y hours"; whether that yields data skipping is the
 * customer's own validation (query-pattern dependent, deliberately out of scope).
 *
 * Output rows: `(metric, dimension, value)` where `dimension` is set only for per-key depth rows.
 * Key metrics: `coverage_bytes_pct` / `coverage_files_pct` (fraction whose leading-key range was
 * clustered under the current config), `depth_*` (Snowflake-style stabbing depth, global and over
 * the covered region), and `unclustered_tail_hours` (age of the oldest not-yet-clustered data).
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
    val qualified = s"$cat.$tableArg"

    val props = sparkSession.sql(s"SHOW TBLPROPERTIES $qualified").collect()
      .map(r => r.getString(0) -> r.getString(1)).toMap
    val keys = props.get(KEYS_PROP)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Seq.empty)

    val out = mutable.ArrayBuffer[Row]()
    def emit(metric: String, value: String): Unit = out += Row(metric, null, value)
    def emitDim(metric: String, dim: String, value: String): Unit = out += Row(metric, dim, value)

    if (keys.isEmpty) {
      emit("clustering_configured", "false")
      return out.toSeq
    }
    emit("clustering_configured", "true")

    val sortMode = props.getOrElse(SORT_MODE_PROP, DEFAULT_SORT_MODE)
    val cfgId = configId(keys, sortMode)
    emit("config_id", cfgId)
    emit("keys", keys.mkString(","))
    emit("sort_mode", sortMode)

    val leadKey = keys.head
    val leadType = sparkSession.table(qualified).schema(leadKey).dataType.sql
    val current = parseState(props.getOrElse(STATE_PROP, "")).filter(_.config == cfgId)

    // One pass over manifest metrics: per-file bytes, whether the leading-key range is covered by a
    // current-config interval, and per-key bounds (typed) for depth.
    val loExpr = metricExpr(leadKey, "lower_bound")
    val hiExpr = metricExpr(leadKey, "upper_bound")
    val coveredCol = coveragePredicate(loExpr, hiExpr, current, leadType)
    val boundCols = keys.flatMap { k =>
      Seq(s"${metricExpr(k, "lower_bound")} AS `${k}__lo`",
        s"${metricExpr(k, "upper_bound")} AS `${k}__hi`")
    }
    val files = sparkSession.sql(
      s"""SELECT file_size_in_bytes AS bytes, coalesce($coveredCol, false) AS covered,
         |  ($loExpr IS NULL OR $hiExpr IS NULL) AS lead_null, ${boundCols.mkString(", ")}
         |FROM $qualified.files""".stripMargin).collect()

    val bytesTotal = files.map(_.getLong(0)).sum
    val bytesCovered = files.filter(_.getBoolean(1)).map(_.getLong(0)).sum
    val nullBytes = files.filter(_.getBoolean(2)).map(_.getLong(0)).sum
    emit("files_total", files.length.toString)
    emit("files_covered", files.count(_.getBoolean(1)).toString)
    emit("bytes_total", bytesTotal.toString)
    emit("bytes_covered", bytesCovered.toString)
    emit("coverage_bytes_pct", pct(bytesCovered, bytesTotal))
    emit("coverage_files_pct", pct(files.count(_.getBoolean(1)).toLong, files.length.toLong))
    emit("null_bound_bytes_pct", pct(nullBytes, bytesTotal))

    // Depth per clustering dimension: global and over the covered region only (the SLA input).
    keys.zipWithIndex.foreach { case (k, i) =>
      val loIdx = 3 + i * 2
      val hiIdx = loIdx + 1
      val all = files.flatMap(r => bounds(r, loIdx, hiIdx))
      val cov = files.filter(_.getBoolean(1)).flatMap(r => bounds(r, loIdx, hiIdx))
      val gd = depth(all)
      val cd = depth(cov)
      emitDim("depth_avg", k, fmt(gd.avg))
      emitDim("depth_p90", k, fmt(gd.p90))
      emitDim("depth_max", k, gd.max.toString)
      emitDim("depth_avg_covered", k, fmt(cd.avg))
      emitDim("depth_p90_covered", k, fmt(cd.p90))
    }

    emit("unclustered_tail_hours", tailHours(sparkSession, qualified, props.get(HWM_PROP)))
    emit("state", props.getOrElse(STATE_PROP, "[]"))
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

  /** Stabbing-depth stats over a set of `[lower, upper]` intervals of one (Comparable) type. */
  def depth(intervals: Seq[(Any, Any)]): DepthStats = {
    if (intervals.isEmpty) return DepthStats(0.0, 0.0, 0L)
    // +1 at each lower, -1 past each upper; at a tie, starts (+1) precede ends (-1) so touching
    // intervals count as overlapping. Sample the running depth at each start event.
    val events = intervals.flatMap { case (lo, hi) => Seq((lo, 1), (hi, -1)) }
    val sorted = events.sortWith { (a, b) =>
      val c = a._1.asInstanceOf[Comparable[Any]].compareTo(b._1)
      if (c != 0) c < 0 else a._2 > b._2
    }
    var cur = 0L
    var maxD = 0L
    val starts = mutable.ArrayBuffer[Long]()
    sorted.foreach { case (_, d) =>
      cur += d
      if (d == 1) {
        starts += cur
        if (cur > maxD) maxD = cur
      }
    }
    DepthStats(starts.sum.toDouble / starts.size, percentile(starts.toSeq, 0.9), maxD)
  }

  private def percentile(values: Seq[Long], p: Double): Double = {
    if (values.isEmpty) return 0.0
    val s = values.sorted
    val idx = math.max(0, math.min(s.length - 1, math.ceil(p * s.length).toInt - 1))
    s(idx).toDouble
  }

  /** A file's [lower, upper] pair at the given row positions; None if either is null. */
  private def bounds(r: Row, loIdx: Int, hiIdx: Int): Option[(Any, Any)] =
    if (r.isNullAt(loIdx) || r.isNullAt(hiIdx)) None else Some((r.get(loIdx), r.get(hiIdx)))

  private def pct(part: Long, total: Long): String =
    if (total == 0) "0.0" else fmt(100.0 * part / total)

  private def fmt(d: Double): String = f"$d%.2f"

  /**
   * Age in hours of the oldest not-yet-clustered data: the oldest non-replace snapshot committed
   * after the watermark snapshot. `0` if nothing is newer than the watermark; `unknown` if the
   * watermark is unset or has been expired (so an SLA breach is never hidden).
   */
  private def tailHours(spark: SparkSession, qualified: String, hwm: Option[String]): String = {
    hwm match {
      case None => "unknown"
      case Some(h) =>
        val floor = spark.sql(
          s"SELECT committed_at FROM $qualified.snapshots WHERE snapshot_id = $h").collect()
        if (floor.isEmpty) return "unknown" // expired watermark
        val rows = spark.sql(
          s"""SELECT CAST((unix_timestamp(current_timestamp()) -
             |  unix_timestamp(min(committed_at))) / 3600.0 AS DOUBLE)
             |FROM $qualified.snapshots
             |WHERE operation != 'replace'
             |  AND committed_at > (SELECT committed_at FROM $qualified.snapshots
             |    WHERE snapshot_id = $h)""".stripMargin).collect()
        if (rows.isEmpty || rows.head.isNullAt(0)) "0.0" else fmt(rows.head.getDouble(0))
    }
  }
}
