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

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.internal.SQLConf

/**
 * The logical plan of the VACUUM command, which runs Iceberg table maintenance by
 * delegating to the catalog's stored procedures. For example:
 * {{{
 *   VACUUM multi_part_name [REMOVE ORPHAN FILES] [RETAIN number HOURS]
 * }}}
 *
 * Snapshot expiration always runs. When `REMOVE ORPHAN FILES` is specified, orphan-file
 * deletion runs afterwards so it cleans up against the settled live-file set. The retention
 * window bounding both operations comes from `RETAIN n HOURS` when given; otherwise each
 * operation falls back to its own SQL-conf default
 * ([[SQLConf.VACUUM_EXPIRE_SNAPSHOTS_RETAIN_HOURS]] and
 * [[SQLConf.VACUUM_REMOVE_ORPHAN_FILES_RETAIN_HOURS]]).
 *
 * The command is thin sugar: it resolves the target catalog and issues the equivalent
 * `CALL` statements, so procedure resolution, argument binding, and any catalog-side
 * authorization all happen on the existing `CALL` path.
 */
case class VacuumTableCommand(
    nameParts: Seq[String],
    removeOrphanFiles: Boolean,
    retainHours: Option[Int]) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val conf = sparkSession.sessionState.conf
    val catalogManager = sparkSession.sessionState.catalogManager
    val (catalog, table) = nameParts match {
      case head +: tail if tail.nonEmpty && catalogManager.isCatalogRegistered(head) =>
        (head, tail)
      case _ =>
        (catalogManager.currentCatalog.name, nameParts)
    }
    val cat = quoteIfNeeded(catalog)
    val tableArg = table.map(quoteIfNeeded).mkString(".")
    val zone = ZoneId.of(conf.sessionLocalTimeZone)
    val now = Instant.now()

    // Snapshot expiration always runs. Procedure arguments must be foldable, so the retention
    // window is resolved here to a literal `older_than` timestamp (now - n hours) in the session
    // time zone rather than passed as a `current_timestamp()` expression, which CALL binding
    // rejects. An explicit RETAIN overrides the conf default.
    val expireCutoff = VacuumTableCommand.olderThan(
      now, retainHours.getOrElse(conf.getConf(SQLConf.VACUUM_EXPIRE_SNAPSHOTS_RETAIN_HOURS)), zone)
    sparkSession.sql(
      s"CALL $cat.system.expire_snapshots(" +
        s"table => '$tableArg', older_than => TIMESTAMP '$expireCutoff')").collect()

    if (removeOrphanFiles) {
      val orphanCutoff = VacuumTableCommand.olderThan(
        now, retainHours.getOrElse(conf.getConf(SQLConf.VACUUM_REMOVE_ORPHAN_FILES_RETAIN_HOURS)),
        zone)
      sparkSession.sql(
        s"CALL $cat.system.remove_orphan_files(" +
          s"table => '$tableArg', older_than => TIMESTAMP '$orphanCutoff')").collect()
    }
    Seq.empty[Row]
  }
}

object VacuumTableCommand {
  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  /**
   * Renders the `older_than` cutoff for a retention window of `retainHours` hours before `now`,
   * as a literal timestamp string in the given time zone. Exposed for testing.
   */
  def olderThan(now: Instant, retainHours: Int, zone: ZoneId): String = {
    val cutoff = now.minus(retainHours.toLong, ChronoUnit.HOURS)
    timestampFormatter.withZone(zone).format(cutoff)
  }
}
