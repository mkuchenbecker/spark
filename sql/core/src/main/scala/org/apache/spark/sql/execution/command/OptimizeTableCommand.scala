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
 * The logical plan of the OPTIMIZE command, which runs Iceberg table maintenance by
 * delegating to the catalog's stored procedures. For example:
 * {{{
 *   OPTIMIZE multi_part_name [REWRITE MANIFESTS]
 * }}}
 *
 * Bin-pack compaction of data files (`system.rewrite_data_files`) always runs. When
 * `REWRITE MANIFESTS` is specified, manifest compaction (`system.rewrite_manifests`) runs
 * afterwards, so it operates over the post-compaction file layout. Each is a separate Iceberg
 * commit, which is why manifest rewrite is opt-in rather than bundled by default. Snapshot
 * expiration is intentionally not part of OPTIMIZE -- that is the VACUUM command's job.
 *
 * The command is thin sugar: it resolves the target catalog and issues the equivalent
 * `CALL` statements, so procedure resolution, argument binding, and any catalog-side
 * authorization all happen on the existing `CALL` path.
 */
case class OptimizeTableCommand(
    nameParts: Seq[String],
    rewriteManifests: Boolean) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalogManager = sparkSession.sessionState.catalogManager
    val (catalog, table) = nameParts match {
      case head +: tail if tail.nonEmpty && catalogManager.isCatalogRegistered(head) =>
        (head, tail)
      case _ =>
        (catalogManager.currentCatalog.name, nameParts)
    }
    OptimizeTableCommand.callStatements(catalog, table, rewriteManifests)
      .foreach(stmt => sparkSession.sql(stmt).collect())
    Seq.empty[Row]
  }
}

object OptimizeTableCommand {
  /**
   * Builds the `CALL` statements that an OPTIMIZE invocation expands into. Data-file compaction
   * is always emitted first; manifest compaction is appended when requested so it runs after the
   * data files have been rewritten. Exposed for testing.
   */
  def callStatements(
      catalog: String,
      table: Seq[String],
      rewriteManifests: Boolean): Seq[String] = {
    val cat = quoteIfNeeded(catalog)
    val tableArg = table.map(quoteIfNeeded).mkString(".")
    val rewriteDataFiles =
      s"CALL $cat.system.rewrite_data_files(table => '$tableArg')"
    if (rewriteManifests) {
      Seq(
        rewriteDataFiles,
        s"CALL $cat.system.rewrite_manifests(table => '$tableArg')")
    } else {
      Seq(rewriteDataFiles)
    }
  }
}
