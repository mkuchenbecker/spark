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

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/**
 * End-to-end behavior test for VACUUM against a real Apache Iceberg table (Hadoop catalog).
 * Verifies the command's actual effects -- snapshot expiration and orphan-file removal --
 * not just the CALLs it emits.
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

  /** Visible (non-checksum) file names directly under a table's data directory. */
  private def dataFileNames(table: String): Set[String] = {
    val dataDir = new Path(s"file:$warehouse/$table/data")
    val fs = dataDir.getFileSystem(spark.sessionState.newHadoopConf())
    fs.listStatus(dataDir).map(_.getPath.getName).toSet
  }

  test("VACUUM expires old snapshots on a real Iceberg table") {
    sql("CREATE TABLE ice.db.t1 (id INT) USING iceberg")
    sql("INSERT INTO ice.db.t1 VALUES (1)")
    sql("INSERT INTO ice.db.t1 VALUES (2)")
    sql("INSERT INTO ice.db.t1 VALUES (3)")
    assert(sql("SELECT * FROM ice.db.t1.snapshots").count() === 3,
      "three inserts should produce three snapshots")

    sql("VACUUM ice.db.t1 RETAIN 0 HOURS")

    assert(sql("SELECT * FROM ice.db.t1.snapshots").count() === 1,
      "expiration should retain only the current snapshot")
    checkAnswer(sql("SELECT id FROM ice.db.t1 ORDER BY id"), Seq(Row(1), Row(2), Row(3)))
  }

  test("VACUUM REMOVE ORPHAN FILES removes only the orphan and keeps the table readable") {
    sql("CREATE TABLE ice.db.t2 (id INT) USING iceberg")
    sql("INSERT INTO ice.db.t2 VALUES (1)")
    sql("INSERT INTO ice.db.t2 VALUES (2)")

    // Data files the table actually references, per Iceberg's `.files` metadata table.
    val liveFiles = sql("SELECT file_path FROM ice.db.t2.files").collect()
      .map(r => new Path(r.getString(0)).getName).toSet
    assert(liveFiles.size === 2, s"expected two referenced data files, got $liveFiles")

    // Plant an unreferenced (orphan) data file, aged past Iceberg's 24h OFD safety window.
    val dataDir = new Path(s"file:$warehouse/db/t2/data")
    val fs = dataDir.getFileSystem(spark.sessionState.newHadoopConf())
    val orphan = new Path(dataDir, "orphan-0000.parquet")
    fs.create(orphan).close()
    fs.setTimes(orphan, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(25), -1)

    val before = dataFileNames("db/t2")
    assert(before.contains("orphan-0000.parquet"))
    assert(liveFiles.subsetOf(before), "referenced files should be present before VACUUM")

    sql("VACUUM ice.db.t2 REMOVE ORPHAN FILES RETAIN 24 HOURS")

    val after = dataFileNames("db/t2")
    // Correct files deleted: exactly the orphan is gone, every referenced file survives.
    val removed = before -- after
    assert(!after.contains("orphan-0000.parquet"), "orphan file should be deleted")
    assert(liveFiles.subsetOf(after),
      s"referenced files must be preserved, missing: ${liveFiles -- after}")
    assert(removed === Set("orphan-0000.parquet"),
      s"only the orphan should be removed, also removed: ${removed - "orphan-0000.parquet"}")

    // Table remains fully readable after orphan-file deletion.
    checkAnswer(sql("SELECT id FROM ice.db.t2 ORDER BY id"), Seq(Row(1), Row(2)))
  }
}
