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

    val props = sparkSession.sql(s"SHOW TBLPROPERTIES $qualified").collect()
      .map(r => r.getString(0) -> r.getString(1)).toMap
    val config = parseClusterConfig(props)

    if (config.keys.isEmpty) {
      // No clustering configured: plain bin-pack compaction (unchanged historical behavior).
      sparkSession.sql(s"CALL $cat.system.rewrite_data_files(table => '$tableArg')").collect()
    } else {
      cluster(sparkSession, cat, tableArg, qualified, config)
    }

    if (rewriteManifests) {
      // Independent manifest compaction; runs after the data rewrite so it sees the new layout.
      sparkSession.sql(s"CALL $cat.system.rewrite_manifests(table => '$tableArg')").collect()
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
      config: ClusterConfig): Unit = {
    import config.{hwm, keys, maxCommits, minAgeMinutes, sortMode, state}

    // Age floor: the newest snapshot at least `minAgeMinutes` old, by commit time. Everything
    // younger is held back so we never rewrite files a concurrent streaming writer is extending.
    val ageFloor = spark.sql(
      s"""SELECT snapshot_id FROM $qualified.snapshots
         |WHERE committed_at <= current_timestamp() - INTERVAL $minAgeMinutes MINUTES
         |ORDER BY committed_at DESC LIMIT 1""".stripMargin).collect().headOption.map(_.getLong(0))
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
    // A since-expired watermark yields None here -> a full backfill up to the floor.
    val hwmMax =
      if (full) None
      else hwm.flatMap { h =>
        try leadKeyMax(spark, qualified, leadKey, Some(h)) catch { case NonFatal(_) => None }
      }

    // No-op if the leading key has not advanced past the previous watermark. Compare numerics by
    // value so a leading key promoted between runs (INT -> BIGINT: a boxed Integer watermark max vs
    // a Long current max) still compares rather than throwing a ClassCastException; date/timestamp/
    // string keys (which don't change type under an Iceberg promotion) use natural ordering.
    val advanced = hwmMax.forall { lo =>
      (floorMax.get, lo) match {
        case (x: Number, y: Number) => BigDecimal(x.toString) > BigDecimal(y.toString)
        case (x, y) => x.asInstanceOf[Comparable[Any]].compareTo(y) > 0
      }
    }
    if (!advanced) return

    // The `where` slice to recluster: `lead <= floorMax`, plus `lead > hwmMax` for an incremental
    // run. Catalyst renders each key-type literal correctly, then the predicate is embedded as
    // a SQL string literal so its own quotes (string / timestamp / date literals) survive the CALL.
    val lead = col(quoteIfNeeded(leadKey))
    val scope = hwmMax.map(lo => (lead > lit(lo)) && (lead <= lit(floorMax.get)))
      .getOrElse(lead <= lit(floorMax.get)).expr.sql
    val cols = keys.map(quoteIfNeeded).mkString(", ")
    val sortOrder = if (sortMode.equalsIgnoreCase("zorder")) s"zorder($cols)" else cols

    // Scoped sort / z-order rewrite with partial progress: min-input-files=1 + rewrite-all=true
    // cluster the region regardless of file count (optimization, not compaction);
    // use-starting-sequence-number keeps concurrent equality-deletes valid. rewrite-all forces a
    // rewrite over the non-empty scope, so a healthy run always commits a snapshot; if none is
    // committed the rewrite failed systemically (partial progress swallows per-group failures), so
    // fail loudly and leave the watermark unadvanced for a retry.
    val snapshotsBefore = snapshotCount(spark, qualified)
    spark.sql(
      s"CALL $cat.system.rewrite_data_files(" +
        s"table => '$tableArg', " +
        "strategy => 'sort', " +
        s"sort_order => '$sortOrder', " +
        s"where => ${Literal(scope).sql}, " +
        "options => map(" +
        "'min-input-files', '1', " +
        "'rewrite-all', 'true', " +
        "'use-starting-sequence-number', 'true', " +
        "'partial-progress.enabled', 'true', " +
        s"'partial-progress.max-commits', '$maxCommits'))").collect()
    if (snapshotCount(spark, qualified) <= snapshotsBefore) {
      throw new IllegalStateException(
        s"OPTIMIZE clustered no data for '$qualified' despite a non-empty scope: z-order " +
          s"(sort-mode=$sortMode) cannot be applied to some column types (e.g. decimal). " +
          s"Keys=[${keys.mkString(",")}], sort-mode=$sortMode.")
    }

    // Advance all clustering metadata in one ALTER TABLE -- the watermark (the consumed age floor,
    // not head), the config id, and the interval state -- so they never disagree across a crash.
    val cfgId = configId(keys, sortMode)
    val newState = advanceState(
      state, cfgId, keys, sortMode, hwmMax.map(_.toString), floorMax.get.toString, full)
    spark.sql(
      s"ALTER TABLE $cat.$tableArg SET TBLPROPERTIES (" +
        s"'$HWM_PROP' = '$floorId', " +
        s"'$CONFIG_ID_PROP' = '$cfgId', " +
        s"'$STATE_PROP' = ${Literal(renderState(newState)).sql})").collect()
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

  /**
   * Clustering configuration resolved from the `optimize.cluster.*` table properties, with every
   * default applied and every value parsed to its type. Resolved once so the rewrite path takes
   * typed parameters instead of re-reading and re-parsing a raw property map.
   */
  case class ClusterConfig(
      keys: Seq[String],
      sortMode: String,
      minAgeMinutes: Long,
      maxCommits: Long,
      hwm: Option[Long],
      state: Seq[ClusterInterval])

  /** Parse the `optimize.cluster.*` table properties into a typed [[ClusterConfig]]. */
  def parseClusterConfig(props: Map[String, String]): ClusterConfig = ClusterConfig(
    keys = props.get(KEYS_PROP)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Seq.empty),
    sortMode = props.getOrElse(SORT_MODE_PROP, DEFAULT_SORT_MODE),
    minAgeMinutes = props.get(MIN_SNAPSHOT_AGE_PROP).map(_.toLong)
      .getOrElse(DEFAULT_MIN_SNAPSHOT_AGE_MINUTES),
    maxCommits = props.get(MAX_COMMITS_PROP).map(_.toLong).getOrElse(DEFAULT_MAX_COMMITS),
    hwm = props.get(HWM_PROP).map(_.toLong),
    state = parseState(props.getOrElse(STATE_PROP, "")))

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

  private def snapshotCount(spark: SparkSession, qualified: String): Long =
    spark.sql(s"SELECT count(*) FROM $qualified.snapshots").collect().head.getLong(0)

  private def dataFileCount(spark: SparkSession, qualified: String): Long =
    spark.sql(s"SELECT count(*) FROM $qualified.files").collect().head.getLong(0)

  private def leadKeyMax(
      spark: SparkSession,
      qualified: String,
      key: String,
      snapshotId: Option[Long]): Option[Any] = {
    val asOf = snapshotId.map(s => s" VERSION AS OF $s").getOrElse("")
    val rows = spark.sql(s"SELECT max(${quoteIfNeeded(key)}) FROM $qualified$asOf").collect()
    rows.headOption.flatMap(r => Option(r.get(0)))
  }

}
