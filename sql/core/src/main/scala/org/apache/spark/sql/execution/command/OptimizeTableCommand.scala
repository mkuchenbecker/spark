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

import scala.util.control.NonFatal

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.functions.{col, lit}

/**
 * The logical plan of the OPTIMIZE command, which runs Iceberg table maintenance by delegating to
 * the catalog's stored procedures. For example:
 * {{{
 *   OPTIMIZE multi_part_name [FULL] [REWRITE MANIFESTS]
 * }}}
 *
 * Behavior depends on whether clustering keys are configured via table properties:
 *
 *  - '''No `optimize.cluster.keys`''': plain bin-pack compaction (`system.rewrite_data_files` with
 *    defaults). This is the historical behavior and is unaffected by `FULL`.
 *
 *  - '''Clustering configured''': the table is (re)clustered by the configured keys using a sort /
 *    z-order rewrite. To stay cheap and to avoid transaction conflicts with concurrent streaming
 *    writers, the rewrite is:
 *      - '''incremental by default''' -- only the forward slice of the leading clustering key that
 *        arrived since the last run (tracked by an `optimize.cluster.hwm-snapshot-id` watermark) is
 *        reclustered; `FULL` reclusters everything up to the age floor (backfill);
 *      - '''held back''' -- snapshots younger than `optimize.cluster.min-snapshot-age-minutes` are
 *        never consumed, so the actively-appended tail is left alone;
 *      - '''committed incrementally''' -- Iceberg partial progress bounds the number of snapshots
 *        per run regardless of how much is rewritten.
 *
 * All tuning lives in `optimize.*` table properties with defaults, so the command surface stays
 * small. The command is thin sugar: it resolves the target catalog and issues the equivalent
 * `CALL` statements, so procedure resolution, argument binding, and any catalog-side authorization
 * all happen on the existing `CALL` path.
 *
 * `REWRITE MANIFESTS` (`system.rewrite_manifests`) is independent and, when specified, runs after
 * the data rewrite so it operates over the post-rewrite file layout. Snapshot expiration is
 * intentionally not part of OPTIMIZE -- that is the VACUUM command's job.
 */
case class OptimizeTableCommand(
    nameParts: Seq[String],
    full: Boolean,
    rewriteManifests: Boolean) extends LeafRunnableCommand {

  import OptimizeTableCommand._

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

    val props = tableProperties(sparkSession, qualified)
    val keys = props.get(KEYS_PROP)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
      .getOrElse(Seq.empty)

    if (keys.isEmpty) {
      // No clustering configured: plain bin-pack compaction (unchanged historical behavior).
      sparkSession.sql(compactionCall(cat, tableArg)).collect()
    } else {
      cluster(sparkSession, cat, tableArg, qualified, props, keys)
    }

    if (rewriteManifests) {
      sparkSession.sql(rewriteManifestsCall(cat, tableArg)).collect()
    }
    Seq.empty[Row]
  }

  private def cluster(
      spark: SparkSession,
      cat: String,
      tableArg: String,
      qualified: String,
      props: Map[String, String],
      keys: Seq[String]): Unit = {
    val sortMode = props.getOrElse(SORT_MODE_PROP, DEFAULT_SORT_MODE)
    val minAgeMinutes = props.get(MIN_SNAPSHOT_AGE_PROP).map(_.toLong)
      .getOrElse(DEFAULT_MIN_SNAPSHOT_AGE_MINUTES)
    val maxCommits = props.get(MAX_COMMITS_PROP).map(_.toLong).getOrElse(DEFAULT_MAX_COMMITS)
    val hwm = props.get(HWM_PROP).map(_.toLong)

    // Age floor: the newest snapshot at least `minAgeMinutes` old, by commit time. Everything
    // younger is held back so we never rewrite files a concurrent streaming writer is extending.
    val ageFloor = ageFloorSnapshot(spark, qualified, minAgeMinutes)
    if (ageFloor.isEmpty) return // nothing old enough to consume yet -> no-op

    val floorId = ageFloor.get
    // Incremental run whose watermark has not moved -> nothing new to do.
    if (!full && hwm.contains(floorId)) return

    // The forward slice is bounded on the leading (primary / temporal) clustering key. Its upper
    // bound is the max value present as of the age floor; Iceberg satisfies this from manifest
    // metrics (aggregate pushdown) when the table has no deletes, so it is metadata-only.
    val leadKey = keys.head
    val floorMax = leadKeyMax(spark, qualified, leadKey, Some(floorId))
    if (floorMax.isEmpty) return // no data as of the age floor -> no-op

    // Lower bound: incremental runs skip what a prior run already clustered (the leading-key max at
    // the previous watermark). FULL ignores it and reclusters everything up to the floor. A missing
    // (expired) watermark falls back to a full backfill up to the floor.
    val hwmMax =
      if (full) None else hwm.flatMap(h => leadKeyMaxOrNone(spark, qualified, leadKey, h))

    val predicate = scopePredicate(leadKey, hwmMax, floorMax.get)
    spark.sql(clusterCall(cat, tableArg, sortMode, keys, predicate, maxCommits)).collect()
    // Advance the watermark to the consumed age floor (not head), so held-back snapshots remain
    // unconsumed and are picked up once they age past the floor.
    spark.sql(setHwmCall(cat, tableArg, floorId)).collect()
  }
}

object OptimizeTableCommand {

  val KEYS_PROP = "optimize.cluster.keys"
  val SORT_MODE_PROP = "optimize.cluster.sort-mode"
  val MIN_SNAPSHOT_AGE_PROP = "optimize.cluster.min-snapshot-age-minutes"
  val HWM_PROP = "optimize.cluster.hwm-snapshot-id"
  val MAX_COMMITS_PROP = "optimize.cluster.max-commits"

  val DEFAULT_SORT_MODE = "zorder"
  val DEFAULT_MIN_SNAPSHOT_AGE_MINUTES = 30L
  val DEFAULT_MAX_COMMITS = 10L

  /** The `CALL` for plain bin-pack compaction (no clustering configured). Exposed for testing. */
  def compactionCall(catalog: String, table: String): String =
    s"CALL $catalog.system.rewrite_data_files(table => '$table')"

  /** The `CALL` for manifest compaction. Exposed for testing. */
  def rewriteManifestsCall(catalog: String, table: String): String =
    s"CALL $catalog.system.rewrite_manifests(table => '$table')"

  /** The Iceberg `sort_order` expression for the configured mode and keys. Exposed for testing. */
  def sortOrderExpr(sortMode: String, keys: Seq[String]): String = {
    val cols = keys.map(quoteIfNeeded).mkString(", ")
    if (sortMode.equalsIgnoreCase("zorder")) s"zorder($cols)" else cols
  }

  /** Double single quotes so a value can be embedded in a single-quoted SQL string. */
  def escapeSqlString(s: String): String = s.replace("'", "''")

  /**
   * The clustering `CALL`: a scoped sort / z-order `rewrite_data_files` with partial progress.
   * `min-input-files=1` + `rewrite-all=true` cluster the scoped region regardless of file count
   * (optimization, not compaction); `use-starting-sequence-number=true` keeps concurrent
   * equality-deletes valid; partial progress bounds snapshots per run. Exposed for testing.
   */
  def clusterCall(
      catalog: String,
      table: String,
      sortMode: String,
      keys: Seq[String],
      whereClause: String,
      maxCommits: Long): String = {
    val order = sortOrderExpr(sortMode, keys)
    s"CALL $catalog.system.rewrite_data_files(" +
      s"table => '$table', " +
      "strategy => 'sort', " +
      s"sort_order => '${escapeSqlString(order)}', " +
      s"where => '${escapeSqlString(whereClause)}', " +
      "options => map(" +
      "'min-input-files', '1', " +
      "'rewrite-all', 'true', " +
      "'use-starting-sequence-number', 'true', " +
      "'partial-progress.enabled', 'true', " +
      s"'partial-progress.max-commits', '$maxCommits'))"
  }

  /** The `ALTER TABLE` that advances the clustering watermark. Exposed for testing. */
  def setHwmCall(catalog: String, table: String, snapshotId: Long): String =
    s"ALTER TABLE $catalog.$table SET TBLPROPERTIES ('$HWM_PROP' = '$snapshotId')"

  /**
   * The `where` predicate bounding the leading-key slice to recluster: `lead <= upper`, plus
   * `lead > lower` for incremental runs. Literal formatting is delegated to Catalyst so every key
   * type is rendered correctly. Exposed for testing.
   */
  def scopePredicate(leadKey: String, lower: Option[Any], upper: Any): String = {
    val c = col(quoteIfNeeded(leadKey))
    val upperCond = c <= lit(upper)
    val cond = lower match {
      case Some(lo) => (c > lit(lo)) && upperCond
      case None => upperCond
    }
    cond.expr.sql
  }

  private def tableProperties(spark: SparkSession, qualified: String): Map[String, String] =
    spark.sql(s"SHOW TBLPROPERTIES $qualified").collect()
      .map(r => r.getString(0) -> r.getString(1)).toMap

  private def ageFloorSnapshot(
      spark: SparkSession,
      qualified: String,
      minAgeMinutes: Long): Option[Long] = {
    val rows = spark.sql(
      s"""SELECT snapshot_id FROM $qualified.snapshots
         |WHERE committed_at <= current_timestamp() - INTERVAL $minAgeMinutes MINUTES
         |ORDER BY committed_at DESC LIMIT 1""".stripMargin).collect()
    rows.headOption.map(_.getLong(0))
  }

  private def leadKeyMax(
      spark: SparkSession,
      qualified: String,
      key: String,
      snapshotId: Option[Long]): Option[Any] = {
    val asOf = snapshotId.map(s => s" VERSION AS OF $s").getOrElse("")
    val rows = spark.sql(s"SELECT max(${quoteIfNeeded(key)}) FROM $qualified$asOf").collect()
    rows.headOption.flatMap(r => Option(r.get(0)))
  }

  /** Leading-key max as of a possibly-expired snapshot; None if the snapshot is gone. */
  private def leadKeyMaxOrNone(
      spark: SparkSession,
      qualified: String,
      key: String,
      snapshotId: Long): Option[Any] =
    try {
      leadKeyMax(spark, qualified, key, Some(snapshotId))
    } catch {
      case NonFatal(_) => None // expired watermark -> fall back to a full backfill up to the floor
    }
}
