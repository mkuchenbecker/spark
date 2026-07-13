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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/**
 * End-to-end behavior test for OPTIMIZE against a real Apache Iceberg table (Hadoop catalog).
 * Verifies the command's actual effects -- data-file compaction and manifest rewrite -- not just
 * the CALLs it emits.
 */
class OptimizeIcebergSuite extends QueryTest with SharedSparkSession {

  // Iceberg spawns its own daemon thread pools (lock manager, worker pool) that outlive the
  // session, which the default audit flags as leaks. They are not ours to manage.
  override protected val enableAutoThreadAudit = false

  private val warehouse: String =
    new File(System.getProperty("java.io.tmpdir"), "optimize-iceberg-warehouse").getAbsolutePath

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.ice.type", "hadoop")
    .set("spark.sql.catalog.ice.warehouse", warehouse)

  protected override def beforeAll(): Unit = {
    Utils.deleteRecursively(new File(warehouse))
    super.beforeAll()
  }

  /** Number of data files the table currently references, per Iceberg's `.files` table. */
  private def fileCount(table: String): Long =
    sql(s"SELECT COUNT(*) FROM $table.files").collect().head.getLong(0)

  /** Number of manifest files for the current snapshot, per Iceberg's `.manifests` table. */
  private def manifestCount(table: String): Long =
    sql(s"SELECT COUNT(*) FROM $table.manifests").collect().head.getLong(0)

  // rewrite_data_files bin-packs a group once it has at least `min-input-files` (default 5) small
  // files, so six single-row inserts reliably produce a compactable group.
  private val numInserts = 6

  test("OPTIMIZE compacts small data files on a real Iceberg table") {
    sql("CREATE TABLE ice.db.o1 (id INT) USING iceberg")
    (1 to numInserts).foreach(i => sql(s"INSERT INTO ice.db.o1 VALUES ($i)"))
    assert(fileCount("ice.db.o1") === numInserts,
      s"$numInserts inserts should produce $numInserts data files")

    sql("VACUUM ice.db.o1 RETAIN 0 HOURS") // no-op guard: OPTIMIZE must not depend on expiration
    sql("OPTIMIZE ice.db.o1")

    assert(fileCount("ice.db.o1") < numInserts,
      "compaction should reduce the number of referenced data files")
    checkAnswer(
      sql("SELECT id FROM ice.db.o1 ORDER BY id"),
      (1 to numInserts).map(Row(_)))
  }

  test("OPTIMIZE REWRITE MANIFESTS compacts manifests and keeps the table readable") {
    sql("CREATE TABLE ice.db.o2 (id INT) USING iceberg")
    (1 to numInserts).foreach(i => sql(s"INSERT INTO ice.db.o2 VALUES ($i)"))

    val manifestsBefore = manifestCount("ice.db.o2")
    assert(manifestsBefore > 1,
      s"$numInserts appends should produce more than one manifest, got $manifestsBefore")

    sql("OPTIMIZE ice.db.o2 REWRITE MANIFESTS")

    // Data-file compaction always runs; manifest compaction ran too.
    assert(fileCount("ice.db.o2") < numInserts, "data files should be compacted")
    assert(manifestCount("ice.db.o2") < manifestsBefore,
      "manifest rewrite should reduce the number of manifests")
    checkAnswer(
      sql("SELECT id FROM ice.db.o2 ORDER BY id"),
      (1 to numInserts).map(Row(_)))
  }
}
