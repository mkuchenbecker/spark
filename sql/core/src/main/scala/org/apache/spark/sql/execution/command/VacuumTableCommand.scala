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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.util.quoteIfNeeded

/**
 * The logical plan of the VACUUM command, which runs Iceberg table maintenance by
 * delegating to the catalog's stored procedures. For example:
 * {{{
 *   VACUUM multi_part_name [OFD] [RETAIN number HOURS]
 * }}}
 *
 * Snapshot expiration always runs. When `OFD` is specified, orphan-file deletion runs
 * afterwards. When `RETAIN n HOURS` is specified it bounds both operations via the
 * procedures' `older_than` argument; otherwise each procedure falls back to its own
 * default (the table's snapshot-age property for expiration, and Iceberg's safe
 * retention default for orphan-file deletion).
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
    val catalogManager = sparkSession.sessionState.catalogManager
    val (catalog, table) = nameParts match {
      case head +: tail if tail.nonEmpty && catalogManager.isCatalogRegistered(head) =>
        (head, tail)
      case _ =>
        (catalogManager.currentCatalog.name, nameParts)
    }
    VacuumTableCommand.callStatements(catalog, table, removeOrphanFiles, retainHours)
      .foreach(stmt => sparkSession.sql(stmt).collect())
    Seq.empty[Row]
  }
}

object VacuumTableCommand {
  /**
   * Builds the `CALL` statements that a VACUUM invocation expands into. Snapshot
   * expiration is always emitted first; orphan-file deletion is appended when requested
   * so it runs after expiration has settled the live file set. Exposed for testing.
   */
  def callStatements(
      catalog: String,
      table: Seq[String],
      removeOrphanFiles: Boolean,
      retainHours: Option[Int]): Seq[String] = {
    val cat = quoteIfNeeded(catalog)
    val tableArg = table.map(quoteIfNeeded).mkString(".")
    val olderThan = retainHours
      .map(h => s", older_than => current_timestamp() - INTERVAL $h HOURS")
      .getOrElse("")
    val expireSnapshots =
      s"CALL $cat.system.expire_snapshots(table => '$tableArg'$olderThan)"
    if (removeOrphanFiles) {
      Seq(
        expireSnapshots,
        s"CALL $cat.system.remove_orphan_files(table => '$tableArg'$olderThan)")
    } else {
      Seq(expireSnapshots)
    }
  }
}
