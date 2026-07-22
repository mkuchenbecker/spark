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

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.classic.ColumnConversions.toRichColumn
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.functions.{col, current_timestamp, expr, lit, max}
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
    val qualifiedTableName = s"$cat.$tableArg"
    val tableCatalog = catalogManager.catalog(catalog).asTableCatalog
    val ident = Identifier.of(table.init.toArray, table.last)

    // Snapshot the physical layout before doing any work so we can report the reduction. This is
    // also the first table access, so a missing table fails here naming the table.
    val filesBefore = sparkSession.table(s"$qualifiedTableName.files").count()
    val snapshotsBefore = sparkSession.table(s"$qualifiedTableName.snapshots").count()

    val config = parseClusterConfig(tableCatalog.loadTable(ident).properties().asScala.toMap)

    config.keys match {
      case Seq() =>
        // No clustering configured: plain bin-pack compaction (unchanged historical behavior).
        sparkSession.sql(s"CALL $cat.system.rewrite_data_files(table => '$tableArg')").collect()
      case _ =>
        cluster(sparkSession, cat, tableArg, qualifiedTableName, tableCatalog, ident, config)
    }

    if (rewriteManifests) {
      // Independent manifest compaction; runs after the data rewrite so it sees the new layout.
      sparkSession.sql(s"CALL $cat.system.rewrite_manifests(table => '$tableArg')").collect()
    }

    val filesAfter = sparkSession.table(s"$qualifiedTableName.files").count()
    val snapshotsAfter = sparkSession.table(s"$qualifiedTableName.snapshots").count()
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
      qualifiedTableName: String,
      tableCatalog: TableCatalog,
      ident: Identifier,
      config: ClusterConfig): Unit = {
    import config.{hwm, keys, maxCommits, minAgeMinutes, sortMode, state}

    // Age floor: the newest snapshot at least `minAgeMinutes` old, by commit time. Everything
    // younger is held back so we never rewrite files a concurrent streaming writer is extending.
    val ageFloor = spark.table(s"$qualifiedTableName.snapshots")
      .where(col("committed_at") <= current_timestamp() - expr(s"INTERVAL $minAgeMinutes MINUTES"))
      .orderBy(col("committed_at").desc)
      .limit(1)
      .select("snapshot_id")
      .collect().headOption.map(_.getLong(0))
    if (ageFloor.isEmpty) return // nothing old enough to consume yet -> no-op

    val floorId = ageFloor.get
    // Incremental run whose watermark has not moved -> nothing new to do.
    if (!full && hwm.contains(floorId)) return

    // The forward slice is bounded on the leading (primary / temporal) clustering key. Its upper
    // bound is the max value present as of the age floor; Iceberg satisfies this from manifest
    // metrics (aggregate pushdown) when the table has no deletes, so it is metadata-only.
    val leadKey = keys.head
    val floorMax = Option(
      spark.read.option("snapshot-id", floorId).table(qualifiedTableName)
        .agg(max(col(quoteIfNeeded(leadKey)))).head().get(0))
    if (floorMax.isEmpty) return // no data as of the age floor -> no-op

    // Lower bound: incremental runs skip what a prior run already clustered -- the last-clustered
    // upper of the current key selection, read from the persisted interval state. The state is a
    // table property, so it survives snapshot expiration; taking the bound from it (rather than
    // re-reading max(key) as of the watermark snapshot, which SE can remove) keeps the run
    // incremental on known state instead of silently falling back to a full backfill. FULL ignores
    // the bound and reclusters everything up to the floor. The tradeoff: OPTIMIZE must run at least
    // once per snapshot-expiration window, else data that first arrives below the last-clustered
    // upper within an expired window is only reclustered by a FULL.
    val cfgId = configId(keys, sortMode)
    val lowerValue = state.find(_.config == cfgId).map(_.upper).filterNot(_ => full)

    // Cast the persisted (string) bound back to the leading key's type so a key promoted between
    // runs (e.g. INT -> BIGINT) is compared after a cast, not across boxed types.
    val leadType = spark.table(qualifiedTableName).schema(leadKey).dataType
    val lead = col(quoteIfNeeded(leadKey))
    val lowerBound = lowerValue.map(u => lit(u).cast(leadType))

    // No-op if the leading key has not advanced past the last-clustered upper. Evaluate the
    // comparison in Catalyst (not in Scala) so the cast above governs the ordering.
    val advanced = lowerBound.forall { lb =>
      spark.range(1).select(lit(floorMax.get) > lb).head().getBoolean(0)
    }
    if (!advanced) return

    // The `where` slice to recluster: `lead <= floorMax`, plus `lead > lowerBound` for an
    // incremental run. Catalyst renders each key-type literal correctly, then the predicate is
    // embedded as a SQL string literal so its own quotes (string / timestamp / date literals)
    // survive the CALL.
    val scope = lowerBound.map(lb => (lead > lb) && (lead <= lit(floorMax.get)))
      .getOrElse(lead <= lit(floorMax.get)).expr.sql
    val cols = keys.map(quoteIfNeeded).mkString(", ")
    val sortOrder = sortMode.toLowerCase(Locale.ROOT) match {
      case "zorder" => s"zorder($cols)"
      case _ => cols
    }

    // Scoped sort / z-order rewrite with partial progress: min-input-files=1 + rewrite-all=true
    // cluster the region regardless of file count (optimization, not compaction);
    // use-starting-sequence-number keeps concurrent equality-deletes valid. rewrite-all forces a
    // rewrite over the non-empty scope, so a healthy run always commits a snapshot; if none is
    // committed the rewrite failed systemically (partial progress swallows per-group failures), so
    // fail loudly and leave the watermark unadvanced for a retry.
    val snapshotsBefore = spark.table(s"$qualifiedTableName.snapshots").count()
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
    if (spark.table(s"$qualifiedTableName.snapshots").count() <= snapshotsBefore) {
      throw new IllegalStateException(
        s"OPTIMIZE clustered no data for '$qualifiedTableName': the scoped rewrite " +
          s"(keys=[${keys.mkString(",")}], sort-mode=$sortMode) committed no snapshot despite a " +
          s"non-empty scope. The watermark was left unadvanced so the run can be retried.")
    }

    // Advance all clustering metadata in one atomic alterTable -- the watermark (the consumed age
    // floor, not head), the config id, and the interval state -- so they never disagree across a
    // crash. Setting properties via the catalog API avoids escaping the state JSON into SQL.
    val newState = advanceState(
      state, cfgId, keys, sortMode, lowerValue, floorMax.get.toString, full)
    tableCatalog.alterTable(ident,
      TableChange.setProperty(HWM_PROP, floorId.toString),
      TableChange.setProperty(CONFIG_ID_PROP, cfgId),
      TableChange.setProperty(STATE_PROP, stateMapper.writeValueAsString(newState)))
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

  private val stateMapper = {
    val mapper = new ObjectMapper() with ClassTagExtensions
    mapper.registerModule(DefaultScalaModule)
    // Omit an absent `lower` (None) so the persisted JSON stays compact and stable.
    mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
    mapper
  }

  /**
   * One clustered leading-key interval `(lower, upper]` under a specific key selection (`config`).
   * `lower = None` means unbounded below (a FULL / first backfill). Persisted, alongside the
   * watermark, in the `optimize.cluster.state` table property so it survives snapshot expiration.
   * Serialized to / from that property via Jackson (`stateMapper` / [[parseState]]).
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

  /**
   * Parse interval state. Empty / absent input is no state (a fresh table). Non-empty but
   * unparseable input is a corrupted property, not "no state" -- silently treating it as empty
   * would make OPTIMIZE recluster from scratch and mis-report ANALYZE coverage, so it fails loudly
   * naming the property and how to clear it. Exposed for testing.
   */
  def parseState(json: String): Seq[ClusterInterval] = {
    if (json == null || json.trim.isEmpty) return Seq.empty
    try {
      stateMapper.readValue[Seq[ClusterInterval]](json)
    } catch {
      case NonFatal(e) =>
        throw new IllegalStateException(
          s"Malformed clustering state in table property '$STATE_PROP'; OPTIMIZE cannot tell " +
            s"what is already clustered. Clear the clustering metadata and let the next OPTIMIZE " +
            s"rebuild it: ALTER TABLE <table> UNSET TBLPROPERTIES " +
            s"('$STATE_PROP', '$HWM_PROP', '$CONFIG_ID_PROP'). Value was: $json", e)
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
    (full, existing.find(_.config == cfgId)) match {
      case (true, _) => others :+ ClusterInterval(cfgId, keysStr, mode, None, upper)
      case (false, Some(cur)) => others :+ cur.copy(keys = keysStr, mode = mode, upper = upper)
      case (false, None) => existing :+ ClusterInterval(cfgId, keysStr, mode, lower, upper)
    }
  }

}
