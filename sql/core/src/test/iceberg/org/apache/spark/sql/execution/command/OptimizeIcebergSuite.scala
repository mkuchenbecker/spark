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

  /** Absolute paths of the data files the table currently references, per `.files`. */
  private def dataFiles(table: String): Set[String] =
    sql(s"SELECT file_path FROM $table.files").collect().map(_.getString(0)).toSet

  /** Value of a single table property, or None if unset. */
  private def tblProp(table: String, key: String): Option[String] =
    sql(s"SHOW TBLPROPERTIES $table").collect()
      .find(_.getString(0) == key).map(_.getString(1))

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

  /** Create a clustered table on leading key `ts`; `minAge` gates the snapshot hold-back. */
  private def createClustered(table: String, sortMode: String, minAge: Int): Unit = {
    sql(s"CREATE TABLE $table (ts INT, val INT) USING iceberg")
    sql(s"ALTER TABLE $table SET TBLPROPERTIES (" +
      s"'optimize.cluster.keys'='ts', " +
      s"'optimize.cluster.sort-mode'='$sortMode', " +
      s"'optimize.cluster.min-snapshot-age-minutes'='$minAge')")
  }

  test("OPTIMIZE FULL clusters configured keys, commits, sets the watermark, preserves data") {
    createClustered("ice.db.c1", sortMode = "sort", minAge = 0)
    (1 to 6).foreach(i => sql(s"INSERT INTO ice.db.c1 VALUES ($i, ${i * 10})"))

    // Baseline: six single-row appends -> six data files, no watermark yet.
    assert(dataFileCount("ice.db.c1") === 6, "six inserts should produce six data files")
    assert(tblProp("ice.db.c1", "optimize.cluster.hwm-snapshot-id").isEmpty, "no watermark yet")
    val snapshotsBefore = snapshotCount("ice.db.c1")

    sql("OPTIMIZE ice.db.c1 FULL")

    // A sort rewrite with rewrite-all combines the whole settled slice into fewer files, commits a
    // new snapshot, preserves data, and advances the watermark to the consumed snapshot.
    assert(dataFileCount("ice.db.c1") < 6,
      s"clustering should combine the six files, got ${dataFileCount("ice.db.c1")}")
    assert(snapshotCount("ice.db.c1") > snapshotsBefore, "clustering must commit a new snapshot")
    assert(tblProp("ice.db.c1", "optimize.cluster.hwm-snapshot-id").isDefined,
      "watermark must be set after clustering")
    checkAnswer(sql("SELECT ts, val FROM ice.db.c1 ORDER BY ts"), (1 to 6).map(i => Row(i, i * 10)))
  }

  test("OPTIMIZE incremental reclusters only the forward slice and advances the watermark") {
    createClustered("ice.db.c2", sortMode = "sort", minAge = 0)
    (1 to 6).foreach(i => sql(s"INSERT INTO ice.db.c2 VALUES ($i, ${i * 10})"))

    // Backfill the initial slice (ts <= 6) to one clustered file; capture it and the watermark.
    sql("OPTIMIZE ice.db.c2 FULL")
    val clustered = dataFiles("ice.db.c2")
    assert(clustered.size === 1, s"full clustering should leave one file, got ${clustered.size}")
    val hwm1 = tblProp("ice.db.c2", "optimize.cluster.hwm-snapshot-id")
    assert(hwm1.isDefined, "watermark set after the backfill")

    // Add a forward slice (ts 7..9). Now: one clustered file + three new single-row files.
    (7 to 9).foreach(i => sql(s"INSERT INTO ice.db.c2 VALUES ($i, ${i * 10})"))
    assert(dataFileCount("ice.db.c2") === 4, "one clustered file + three new appends")

    sql("OPTIMIZE ice.db.c2")

    // Incremental scope is ts in (6, 9]: the three forward files are combined, and the already
    // clustered ts<=6 file is left byte-for-byte untouched (its exact path still present).
    val after = dataFiles("ice.db.c2")
    assert(clustered.subsetOf(after),
      s"the already-clustered file must be untouched: $clustered not all in $after")
    assert(dataFileCount("ice.db.c2") === 2,
      s"only the forward slice should be combined, got ${dataFileCount("ice.db.c2")}")
    val hwm2 = tblProp("ice.db.c2", "optimize.cluster.hwm-snapshot-id")
    assert(hwm2.isDefined && hwm2 != hwm1, s"watermark must advance: $hwm1 -> $hwm2")
    checkAnswer(sql("SELECT ts, val FROM ice.db.c2 ORDER BY ts"), (1 to 9).map(i => Row(i, i * 10)))
  }

  test("OPTIMIZE holds back snapshots younger than the age floor (no-op)") {
    // A very large hold-back means no snapshot is old enough to consume.
    createClustered("ice.db.c3", sortMode = "sort", minAge = 60000)
    (1 to 6).foreach(i => sql(s"INSERT INTO ice.db.c3 VALUES ($i, ${i * 10})"))
    val snapshotsBefore = snapshotCount("ice.db.c3")

    sql("OPTIMIZE ice.db.c3")

    assert(snapshotCount("ice.db.c3") === snapshotsBefore,
      "no snapshot is old enough -> OPTIMIZE must not commit anything")
    assert(tblProp("ice.db.c3", "optimize.cluster.hwm-snapshot-id").isEmpty,
      "watermark must stay unset when nothing is consumed")
    checkAnswer(sql("SELECT ts, val FROM ice.db.c3 ORDER BY ts"), (1 to 6).map(i => Row(i, i * 10)))
  }

  test("OPTIMIZE FULL with zorder mode clusters by multiple keys and preserves data") {
    sql("CREATE TABLE ice.db.c4 (k1 INT, k2 INT, val INT) USING iceberg")
    sql("ALTER TABLE ice.db.c4 SET TBLPROPERTIES (" +
      "'optimize.cluster.keys'='k1,k2', " +
      "'optimize.cluster.sort-mode'='zorder', " +
      "'optimize.cluster.min-snapshot-age-minutes'='0')")
    (1 to 6).foreach(i => sql(s"INSERT INTO ice.db.c4 VALUES ($i, ${7 - i}, ${i * 10})"))
    val snapshotsBefore = snapshotCount("ice.db.c4")

    sql("OPTIMIZE ice.db.c4 FULL")

    assert(snapshotCount("ice.db.c4") > snapshotsBefore, "z-order clustering must commit")
    assert(dataFileCount("ice.db.c4") < 6, "z-order rewrite should combine the files")
    checkAnswer(sql("SELECT k1, k2, val FROM ice.db.c4 ORDER BY k1"),
      (1 to 6).map(i => Row(i, 7 - i, i * 10)))
  }
}
