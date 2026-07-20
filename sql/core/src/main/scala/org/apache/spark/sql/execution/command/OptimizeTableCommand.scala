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

import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.zip.CRC32

import scala.collection.mutable
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType

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

  override lazy val output: Seq[Attribute] = Seq(
    AttributeReference("metric", StringType, nullable = false)(),
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

    // Snapshot the physical layout before doing any work so we can report the reduction. This is
    // also the first table access, so a missing table fails here naming the table.
    val filesBefore = dataFileCount(sparkSession, qualified)
    val snapshotsBefore = snapshotCount(sparkSession, qualified)

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

    val filesAfter = dataFileCount(sparkSession, qualified)
    val snapshotsAfter = snapshotCount(sparkSession, qualified)
    Seq(
      Row("files_before", filesBefore.toString),
      Row("files_after", filesAfter.toString),
      Row("files_removed", (filesBefore - filesAfter).toString),
      Row("snapshots_committed", (snapshotsAfter - snapshotsBefore).toString))
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

    // True no-op: an incremental run whose leading key has not advanced past the previous watermark
    // has nothing to recluster; leave the watermark in place rather than churning an empty rewrite.
    if (hwmMax.exists(lo => !valueGt(floorMax.get, lo))) return

    val predicate = scopePredicate(leadKey, hwmMax, floorMax.get)
    // The scope is non-empty (data exists at/below floorMax) and `rewrite-all` forces a rewrite, so
    // a healthy run always commits a new snapshot. If nothing committed, the rewrite failed
    // systemically -- partial progress swallows per-group failures, which would otherwise leave a
    // silent no-op. Fail loudly and leave the watermark unadvanced so the run is retryable.
    val snapshotsBefore = snapshotCount(spark, qualified)
    spark.sql(clusterCall(cat, tableArg, sortMode, keys, predicate, maxCommits)).collect()
    if (snapshotCount(spark, qualified) <= snapshotsBefore) {
      throw new IllegalStateException(
        s"OPTIMIZE clustered no data for '$qualified': the rewrite committed no snapshot despite " +
          "a non-empty scope. This usually means an unsupported clustering configuration -- for " +
          "example, z-order (sort-mode=zorder) on a column type Iceberg cannot z-order, such as " +
          s"decimal. Keys=[${keys.mkString(",")}], sort-mode=$sortMode.")
    }
    // Advance the clustering metadata in a single commit: the watermark (consumed age floor, not
    // head), the current config id, and the interval state that records which leading-key range is
    // now clustered under which key selection. These are one logical fact, so they must land
    // together; a missing (expired) watermark rewrote from -inf, so the epoch has no lower bound.
    val cfgId = configId(keys, sortMode)
    val newState = advanceState(
      parseState(props.getOrElse(STATE_PROP, "")),
      cfgId, keys, sortMode, hwmMax.map(renderValue), renderValue(floorMax.get), full)
    spark.sql(setClusterMetaCall(cat, tableArg, floorId, cfgId, renderState(newState))).collect()
  }
}

object OptimizeTableCommand {

  val KEYS_PROP = "optimize.cluster.keys"
  val SORT_MODE_PROP = "optimize.cluster.sort-mode"
  val MIN_SNAPSHOT_AGE_PROP = "optimize.cluster.min-snapshot-age-minutes"
  val HWM_PROP = "optimize.cluster.hwm-snapshot-id"
  val MAX_COMMITS_PROP = "optimize.cluster.max-commits"
  val CONFIG_ID_PROP = "optimize.cluster.config-id"
  val STATE_PROP = "optimize.cluster.state"

  val DEFAULT_SORT_MODE = "zorder"
  val DEFAULT_MIN_SNAPSHOT_AGE_MINUTES = 30L
  val DEFAULT_MAX_COMMITS = 10L

  private val stateMapper = new ObjectMapper()

  /**
   * One clustered leading-key interval `(lower, upper]` under a specific key selection (`config`).
   * `lower = None` means unbounded below (a FULL / first backfill). Persisted, alongside the
   * watermark, in the `optimize.cluster.state` table property so it survives snapshot expiration.
   */
  case class ClusterInterval(
      config: String, keys: String, mode: String, lower: Option[String], upper: String)

  /** Stable, compact identity of a key selection: only a keys/mode change produces a new id. */
  def configId(keys: Seq[String], sortMode: String): String = {
    val normalized = keys.map(_.trim).mkString(",") + "|" + sortMode.toLowerCase(Locale.ROOT)
    val crc = new CRC32()
    crc.update(normalized.getBytes(StandardCharsets.UTF_8))
    java.lang.Long.toHexString(crc.getValue)
  }

  /** Render a leading-key value for storage; consumers CAST it back to the key type in SQL. */
  def renderValue(v: Any): String = v.toString

  /** Serialize interval state to the JSON stored in `optimize.cluster.state`. For testing. */
  def renderState(intervals: Seq[ClusterInterval]): String = {
    val arr = new java.util.ArrayList[java.util.Map[String, Object]]()
    intervals.foreach { iv =>
      val m = new java.util.LinkedHashMap[String, Object]()
      m.put("config", iv.config)
      m.put("keys", iv.keys)
      m.put("mode", iv.mode)
      iv.lower.foreach(l => m.put("lower", l))
      m.put("upper", iv.upper)
      arr.add(m)
    }
    stateMapper.writeValueAsString(arr)
  }

  /** Parse interval state; malformed or empty input is treated as no state. Exposed for testing. */
  def parseState(json: String): Seq[ClusterInterval] = {
    if (json == null || json.trim.isEmpty) return Seq.empty
    try {
      val arr = stateMapper.readValue(json, classOf[java.util.List[java.util.Map[String, String]]])
      val out = mutable.ArrayBuffer[ClusterInterval]()
      val it = arr.iterator()
      while (it.hasNext) {
        val m = it.next().asInstanceOf[java.util.Map[String, String]]
        out += ClusterInterval(
          Option(m.get("config")).getOrElse(""),
          Option(m.get("keys")).getOrElse(""),
          Option(m.get("mode")).getOrElse(""),
          Option(m.get("lower")),
          Option(m.get("upper")).getOrElse(""))
      }
      out.toSeq
    } catch {
      case NonFatal(_) => Seq.empty
    }
  }

  /**
   * Fold a completed run into the interval state. A same-config incremental run extends the current
   * epoch's upper bound (keeping its lower); a config change appends a new epoch, retains the old
   * ones (durable key-selection history); FULL collapses the current config to one unbounded-below
   * interval. Exposed for testing.
   */
  def advanceState(
      existing: Seq[ClusterInterval],
      cfgId: String,
      keys: Seq[String],
      mode: String,
      lower: Option[String],
      upper: String,
      full: Boolean): Seq[ClusterInterval] = {
    val keysStr = keys.mkString(",")
    val others = existing.filterNot(_.config == cfgId)
    if (full) {
      others :+ ClusterInterval(cfgId, keysStr, mode, None, upper)
    } else {
      existing.find(_.config == cfgId) match {
        case Some(cur) => others :+ cur.copy(keys = keysStr, mode = mode, upper = upper)
        case None => existing :+ ClusterInterval(cfgId, keysStr, mode, lower, upper)
      }
    }
  }

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

  /**
   * The clustering `CALL`: a scoped sort / z-order `rewrite_data_files` with partial progress.
   * `min-input-files=1` + `rewrite-all=true` cluster the scoped region regardless of file count
   * (optimization, not compaction); `use-starting-sequence-number=true` keeps concurrent
   * equality-deletes valid; partial progress bounds snapshots per run. The `where` predicate is
   * rendered as a SQL string literal via Catalyst so its own quotes (string / timestamp / date
   * literals) survive the CALL's string-literal parsing. Exposed for testing.
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
      s"sort_order => '$order', " +
      s"where => ${Literal(whereClause).sql}, " +
      "options => map(" +
      "'min-input-files', '1', " +
      "'rewrite-all', 'true', " +
      "'use-starting-sequence-number', 'true', " +
      "'partial-progress.enabled', 'true', " +
      s"'partial-progress.max-commits', '$maxCommits'))"
  }

  /**
   * The single `ALTER TABLE` that advances all clustering metadata at once -- watermark, config id,
   * and interval state -- so they never disagree across a crash. The state JSON is embedded as a
   * Catalyst string literal so its quotes/braces survive parsing. Exposed for testing.
   */
  def setClusterMetaCall(
      catalog: String,
      table: String,
      snapshotId: Long,
      cfgId: String,
      stateJson: String): String =
    s"ALTER TABLE $catalog.$table SET TBLPROPERTIES (" +
      s"'$HWM_PROP' = '$snapshotId', " +
      s"'$CONFIG_ID_PROP' = '$cfgId', " +
      s"'$STATE_PROP' = ${Literal(stateJson).sql})"

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

  /**
   * True if `a > b` for two leading-key values. The two values can be read under different schema
   * versions when the leading key is type-promoted between runs (e.g. INT -> BIGINT): the watermark
   * max comes back boxed as Integer and the current max as Long, which a raw Comparable.compareTo
   * cannot compare (ClassCastException). Numeric values are therefore compared by value; other
   * comparable types (date / timestamp / string, which do not change type under an Iceberg
   * promotion) fall back to natural ordering.
   */
  private def valueGt(a: Any, b: Any): Boolean = (a, b) match {
    case (x: Number, y: Number) => BigDecimal(x.toString) > BigDecimal(y.toString)
    case _ => a.asInstanceOf[Comparable[Any]].compareTo(b) > 0
  }

  private def snapshotCount(spark: SparkSession, qualified: String): Long =
    spark.sql(s"SELECT count(*) FROM $qualified.snapshots").collect().head.getLong(0)

  private def dataFileCount(spark: SparkSession, qualified: String): Long =
    spark.sql(s"SELECT count(*) FROM $qualified.files").collect().head.getLong(0)

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
