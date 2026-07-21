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
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/**
 * End-to-end behavior test for VACUUM against a real Apache Iceberg table (Hadoop catalog).
 *
 * Assertions follow a delta discipline: each check pins a self-defending baseline (so a relative
 * assertion cannot pass vacuously), then asserts the effect plus the invariant that current data
 * is unchanged. Orphan-file deletion is verified by exact per-path existence (the planted orphan
 * is gone; every file the table references, read from `.files`, survives) rather than a fragile
 * directory-listing diff, so checksum sidecar files and listing order cannot make it flaky.
 */
class VacuumIcebergSuite extends QueryTest with SharedSparkSession {

  // Iceberg spawns its own daemon thread pools (lock manager, worker pool) that outlive the
  // session, which the default audit flags as leaks. They are not ours to manage.
  override protected val enableAutoThreadAudit = false

  private val warehouse: String =
    new File(System.getProperty("java.io.tmpdir"), "vacuum-iceberg-warehouse").getAbsolutePath

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

  private def snapshotCount(table: String): Long =
    sql(s"SELECT count(*) FROM $table.snapshots").collect().head.getLong(0)

  /** Absolute paths of the data files the table currently references, per `.files`. */
  private def referencedFiles(table: String): Set[Path] =
    sql(s"SELECT file_path FROM $table.files").collect().map(r => new Path(r.getString(0))).toSet

  private def fs: FileSystem =
    new Path(s"file:$warehouse").getFileSystem(spark.sessionState.newHadoopConf())

  /** Plant an unreferenced data file under `relDir`, aged well past Iceberg's 24h OFD guard. */
  private def plantOrphan(relDir: String, name: String): Path = {
    val orphan = new Path(s"file:$warehouse/$relDir/$name")
    fs.create(orphan).close()
    fs.setTimes(orphan, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(240), -1)
    orphan
  }

  private def messageChain(t: Throwable): String =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)
      .flatMap(e => Option(e.getMessage)).mkString(" | ")

  test("VACUUM expires unreferenced snapshots down to the current one and preserves data") {
    sql("CREATE TABLE ice.db.v1 (id INT) USING iceberg")
    (1 to 3).foreach(i => sql(s"INSERT INTO ice.db.v1 VALUES ($i)"))

    // Baseline: three inserts -> three snapshots. Self-defends the expiration delta below.
    assert(snapshotCount("ice.db.v1") === 3, "three inserts should produce three snapshots")

    sql("VACUUM ice.db.v1 RETAIN 0 HOURS")

    assert(snapshotCount("ice.db.v1") === 1,
      "expiration should retain exactly the current snapshot")
    checkAnswer(sql("SELECT id FROM ice.db.v1 ORDER BY id"), Seq(Row(1), Row(2), Row(3)))
  }

  test("VACUUM REMOVE ORPHAN FILES deletes only the orphan and preserves referenced files + data") {
    sql("CREATE TABLE ice.db.v2 (id INT) USING iceberg")
    sql("INSERT INTO ice.db.v2 VALUES (1)")
    sql("INSERT INTO ice.db.v2 VALUES (2)")

    // Baseline: exactly the two referenced data files, captured by absolute path.
    val referenced = referencedFiles("ice.db.v2")
    assert(referenced.size === 2, s"expected two referenced data files, got $referenced")
    val orphan = plantOrphan("db/v2/data", "orphan-0000.parquet")
    assert(fs.exists(orphan), "planted orphan should exist before VACUUM")

    sql("VACUUM ice.db.v2 REMOVE ORPHAN FILES RETAIN 24 HOURS")

    // Correct files deleted: the orphan is gone; every referenced file (by exact path) survives.
    assert(!fs.exists(orphan), "the orphan file should be deleted")
    referenced.foreach(p =>
      assert(fs.exists(p), s"referenced data file must be preserved: $p"))
    checkAnswer(sql("SELECT id FROM ice.db.v2 ORDER BY id"), Seq(Row(1), Row(2)))
  }

  test("VACUUM without REMOVE ORPHAN FILES leaves orphan files untouched (OFD is opt-in)") {
    sql("CREATE TABLE ice.db.v3 (id INT) USING iceberg")
    sql("INSERT INTO ice.db.v3 VALUES (1)")
    sql("INSERT INTO ice.db.v3 VALUES (2)")
    val orphan = plantOrphan("db/v3/data", "orphan-0000.parquet")
    assert(fs.exists(orphan), "planted orphan should exist before VACUUM")

    // Expiration only -- REMOVE ORPHAN FILES not requested, so orphan-file deletion must not run.
    sql("VACUUM ice.db.v3 RETAIN 0 HOURS")

    assert(fs.exists(orphan),
      "orphan must survive: VACUUM without REMOVE ORPHAN FILES must not delete unreferenced files")
    checkAnswer(sql("SELECT id FROM ice.db.v3 ORDER BY id"), Seq(Row(1), Row(2)))
  }

  test("VACUUM on a missing table fails and names the table") {
    val e = intercept[Exception](sql("VACUUM ice.db.no_such_vacuum_table").collect())
    assert(messageChain(e).contains("no_such_vacuum_table"),
      s"error must identify the missing table: ${messageChain(e).take(300)}")
  }
}
