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
 *
 * Assertions follow a delta discipline: every check pins a self-defending baseline first (so a
 * relative assertion cannot pass vacuously on an empty table), then asserts the DIRECTION of the
 * metadata change plus the invariant that must always hold -- the current data is unchanged. It
 * additionally asserts that OPTIMIZE actually committed work (a new snapshot), so a silent no-op
 * fails loudly rather than masquerading as success. Physical counts are read from Iceberg's
 * `.files` / `.manifests` / `.snapshots` metadata tables, never from raw directory listings.
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

  private def count(query: String): Long = sql(query).collect().head.getLong(0)
  private def snapshotCount(table: String): Long = count(s"SELECT count(*) FROM $table.snapshots")
  private def dataFileCount(table: String): Long = count(s"SELECT count(*) FROM $table.files")
  private def manifestCount(table: String): Long = count(s"SELECT count(*) FROM $table.manifests")

  /** Append `n` single-row files; each single-row insert is one data file on an unpartitioned table. */
  private def seed(table: String, n: Int): Unit =
    (1 to n).foreach(i => sql(s"INSERT INTO $table VALUES ($i)"))

  private def messageChain(t: Throwable): String =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)
      .flatMap(e => Option(e.getMessage)).mkString(" | ")

  test("OPTIMIZE compacts data files, commits a new snapshot, and preserves data") {
    sql("CREATE TABLE ice.db.o1 (id INT) USING iceberg")
    seed("ice.db.o1", 6)

    // Baseline: six single-row inserts -> six data files. Self-defends the compaction delta below.
    assert(dataFileCount("ice.db.o1") === 6, "six inserts should produce six data files")
    val filesBefore = dataFileCount("ice.db.o1")
    val snapshotsBefore = snapshotCount("ice.db.o1")

    sql("OPTIMIZE ice.db.o1")

    // Compaction happened: strictly fewer data files AND a fresh commit (rewrite_data_files only
    // commits when it actually rewrote files, so this catches a silent no-op). OPTIMIZE must not
    // expire snapshots (that is VACUUM's job), so the snapshot count only grows.
    assert(dataFileCount("ice.db.o1") < filesBefore,
      s"compaction should reduce data files: $filesBefore -> ${dataFileCount("ice.db.o1")}")
    assert(snapshotCount("ice.db.o1") > snapshotsBefore,
      "compaction must commit a new snapshot (proves OPTIMIZE did real work, and did not expire)")
    checkAnswer(sql("SELECT id FROM ice.db.o1 ORDER BY id"), (1 to 6).map(Row(_)))
  }

  test("OPTIMIZE REWRITE MANIFESTS compacts manifests and preserves data") {
    sql("CREATE TABLE ice.db.o2 (id INT) USING iceberg")
    seed("ice.db.o2", 6)

    // Baseline: multiple manifests exist to merge (each fast-append writes its own manifest; Iceberg
    // does not auto-merge until far more than six). Self-defends the manifest-reduction delta.
    val manifestsBefore = manifestCount("ice.db.o2")
    assert(manifestsBefore >= 2,
      s"six appends should leave multiple manifests to compact, got $manifestsBefore")
    val snapshotsBefore = snapshotCount("ice.db.o2")

    sql("OPTIMIZE ice.db.o2 REWRITE MANIFESTS")

    assert(manifestCount("ice.db.o2") < manifestsBefore,
      s"manifest rewrite should reduce manifests: $manifestsBefore -> ${manifestCount("ice.db.o2")}")
    assert(dataFileCount("ice.db.o2") < 6, "the mandatory data-file compaction leg must also run")
    assert(snapshotCount("ice.db.o2") > snapshotsBefore, "the rewrites must commit new snapshots")
    checkAnswer(sql("SELECT id FROM ice.db.o2 ORDER BY id"), (1 to 6).map(Row(_)))
  }

  test("OPTIMIZE on a missing table fails and names the table") {
    val e = intercept[Exception](sql("OPTIMIZE ice.db.no_such_optimize_table").collect())
    assert(messageChain(e).contains("no_such_optimize_table"),
      s"error must identify the missing table: ${messageChain(e).take(300)}")
  }
}
