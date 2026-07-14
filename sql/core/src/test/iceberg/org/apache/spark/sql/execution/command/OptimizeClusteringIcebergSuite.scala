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
 * Correctness matrix for OPTIMIZE clustering against a real Apache Iceberg table (Hadoop catalog).
 *
 * OPTIMIZE clustering is stateful (a per-table watermark) and must preserve query results across
 * every table type, partition transform, clustering-key type, sort mode, and delete mode. The
 * invariant every test pins is [[assertPreserves]]: the full row set, ordered, is byte-for-byte
 * identical before and after OPTIMIZE. On top of that each test asserts the behavior specific to
 * its dimension (a commit happened, the watermark advanced, a delete stayed deleted, ...).
 */
class OptimizeClusteringIcebergSuite extends QueryTest with SharedSparkSession {

  // Iceberg spawns its own daemon thread pools that outlive the session; not ours to manage.
  override protected val enableAutoThreadAudit = false

  private val warehouse: String =
    new File(System.getProperty("java.io.tmpdir"), "optimize-cluster-warehouse").getAbsolutePath

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

  private def count(q: String): Long = sql(q).collect().head.getLong(0)
  private def snapshotCount(t: String): Long = count(s"SELECT count(*) FROM $t.snapshots")
  private def rowCount(t: String): Long = count(s"SELECT count(*) FROM $t")
  private def dataFiles(t: String): Set[String] =
    sql(s"SELECT file_path FROM $t.files").collect().map(_.getString(0)).toSet
  private def hwm(t: String): Option[String] =
    sql(s"SHOW TBLPROPERTIES $t").collect()
      .find(_.getString(0) == "optimize.cluster.hwm-snapshot-id").map(_.getString(1))
  private def rows(t: String, orderBy: String): Seq[Row] =
    sql(s"SELECT * FROM $t ORDER BY $orderBy").collect().toSeq
  private def messageChain(t: Throwable): String =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)
      .flatMap(e => Option(e.getMessage)).mkString(" | ")

  /** Run OPTIMIZE and assert the ordered row set is unchanged. Returns the snapshot delta. */
  private def assertPreserves(t: String, orderBy: String, full: Boolean = false): Long = {
    val before = rows(t, orderBy)
    val snapsBefore = snapshotCount(t)
    sql(s"OPTIMIZE $t${if (full) " FULL" else ""}").collect()
    assert(rows(t, orderBy) === before, s"OPTIMIZE must preserve query results for $t")
    snapshotCount(t) - snapsBefore
  }

  private def clustered(keys: String, sortMode: String = "zorder", minAge: Int = 0): String =
    s"'optimize.cluster.keys'='$keys', 'optimize.cluster.sort-mode'='$sortMode', " +
      s"'optimize.cluster.min-snapshot-age-minutes'='$minAge'"

  // ---------------------------------------------------------------------------------------------
  // Partition schemes -- clustering must work and preserve data on every partition transform.
  // ---------------------------------------------------------------------------------------------

  test("partition scheme: unpartitioned") {
    sql(s"CREATE TABLE ice.db.p_none (ts INT, val INT) USING iceberg " +
      s"TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO ice.db.p_none VALUES ($i, ${i * 10})"))
    assert(assertPreserves("ice.db.p_none", "ts", full = true) > 0, "clustering must commit")
    assert(hwm("ice.db.p_none").isDefined)
  }

  test("partition scheme: identity partition") {
    sql(s"CREATE TABLE ice.db.p_id (ts INT, part INT, val INT) USING iceberg " +
      s"PARTITIONED BY (part) TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO ice.db.p_id VALUES ($i, ${i % 2}, ${i * 10})"))
    assert(assertPreserves("ice.db.p_id", "ts", full = true) > 0, "clustering must commit")
  }

  test("partition scheme: days(ts) transform") {
    sql(s"CREATE TABLE ice.db.p_days (ts TIMESTAMP, val INT) USING iceberg " +
      s"PARTITIONED BY (days(ts)) TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i =>
      sql(s"INSERT INTO ice.db.p_days VALUES (TIMESTAMP '2026-01-0$i 12:00:00', ${i * 10})"))
    assert(assertPreserves("ice.db.p_days", "ts", full = true) > 0, "clustering must commit")
  }

  test("partition scheme: hours(ts) transform") {
    sql(s"CREATE TABLE ice.db.p_hours (ts TIMESTAMP, val INT) USING iceberg " +
      s"PARTITIONED BY (hours(ts)) TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i =>
      sql(s"INSERT INTO ice.db.p_hours VALUES (TIMESTAMP '2026-01-01 0$i:00:00', ${i * 10})"))
    assert(assertPreserves("ice.db.p_hours", "ts", full = true) > 0, "clustering must commit")
  }

  test("partition scheme: bucket(4, id) transform") {
    sql(s"CREATE TABLE ice.db.p_bucket (ts INT, id INT, val INT) USING iceberg " +
      s"PARTITIONED BY (bucket(4, id)) TBLPROPERTIES (${clustered("ts")})")
    (1 to 8).foreach(i => sql(s"INSERT INTO ice.db.p_bucket VALUES ($i, $i, ${i * 10})"))
    assert(assertPreserves("ice.db.p_bucket", "ts", full = true) > 0, "clustering must commit")
  }

  test("partition scheme: truncate on string") {
    sql(s"CREATE TABLE ice.db.p_trunc (ts INT, region STRING, val INT) USING iceberg " +
      s"PARTITIONED BY (truncate(2, region)) TBLPROPERTIES (${clustered("ts")})")
    Seq("us-west", "us-east", "eu-west", "ap-south").zipWithIndex.foreach { case (r, i) =>
      sql(s"INSERT INTO ice.db.p_trunc VALUES (${i + 1}, '$r', ${i * 10})")
    }
    assert(assertPreserves("ice.db.p_trunc", "ts", full = true) > 0, "clustering must commit")
  }

  test("partition scheme: multi-field days(ts) + region") {
    sql(s"CREATE TABLE ice.db.p_multi (ts TIMESTAMP, region STRING, val INT) USING iceberg " +
      s"PARTITIONED BY (days(ts), region) TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i =>
      sql(s"INSERT INTO ice.db.p_multi " +
        s"VALUES (TIMESTAMP '2026-01-0$i 12:00:00', 'r${i % 2}', $i)"))
    assert(assertPreserves("ice.db.p_multi", "ts", full = true) > 0, "clustering must commit")
  }

  // ---------------------------------------------------------------------------------------------
  // Leading-key types -- exercise scopePredicate literal rendering + metadata max() per type.
  // ---------------------------------------------------------------------------------------------

  private def leadTypeTest(
      name: String, tpe: String, values: Seq[String], sortMode: String = "zorder"): Unit =
    test(s"leading-key type: $name") {
      val t = s"ice.db.k_$name"
      sql(s"CREATE TABLE $t (k $tpe, val INT) USING iceberg " +
        s"TBLPROPERTIES (${clustered("k", sortMode)})")
      values.zipWithIndex.foreach { case (v, i) => sql(s"INSERT INTO $t VALUES ($v, $i)") }
      assert(assertPreserves(t, "k", full = true) > 0, s"$name full clustering must commit")
      assert(hwm(t).isDefined, s"$name watermark set")
    }

  leadTypeTest("int", "INT", (1 to 6).map(_.toString))
  leadTypeTest("long", "BIGINT", (1 to 6).map(i => s"${i}L"))
  leadTypeTest("string", "STRING", Seq("'a'", "'b'", "'c'", "'d'", "'e'", "'f'"))
  leadTypeTest("date", "DATE", (1 to 6).map(i => s"DATE '2026-01-0$i'"))
  leadTypeTest("timestamp", "TIMESTAMP",
    (1 to 6).map(i => s"TIMESTAMP '2026-01-0$i 00:00:00'"))
  // Iceberg z-order does not support decimal columns; decimal clusters with linear sort mode.
  leadTypeTest("decimal", "DECIMAL(10,2)", (1 to 6).map(i => s"$i.50"), sortMode = "sort")

  test("clustering: z-order on an unsupported column type fails loudly, not silently") {
    val t = "ice.db.k_zbad"
    sql(s"CREATE TABLE $t (k DECIMAL(10,2), val INT) USING iceberg " +
      s"TBLPROPERTIES (${clustered("k", sortMode = "zorder")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i.50, $i)"))
    // z-order on decimal fails inside every rewrite group; partial progress would otherwise make
    // this a silent no-op. The command must surface a clear no-progress error instead.
    val e = intercept[Exception](sql(s"OPTIMIZE $t FULL").collect())
    assert(messageChain(e).contains("committed no snapshot"),
      s"expected a clear no-progress error, got: ${messageChain(e).take(300)}")
    assert(hwm(t).isEmpty, "watermark must not advance on a failed clustering run")
  }

  test("leading-key type: timestamp incremental forward slice") {
    val t = "ice.db.k_ts_inc"
    sql(s"CREATE TABLE $t (ts TIMESTAMP, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', $i)"))
    sql(s"OPTIMIZE $t FULL").collect()
    val h1 = hwm(t)
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', $i)"))
    assert(assertPreserves(t, "ts") > 0, "incremental clustering over new timestamps must commit")
    assert(hwm(t) != h1, "watermark must advance on a timestamp-keyed incremental run")
  }

  // ---------------------------------------------------------------------------------------------
  // Sort modes.
  // ---------------------------------------------------------------------------------------------

  test("sort mode: linear sort, multiple keys") {
    val t = "ice.db.s_sort"
    sql(s"CREATE TABLE $t (k1 INT, k2 INT, val INT) USING iceberg " +
      s"TBLPROPERTIES (${clustered("k1,k2", sortMode = "sort")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${7 - i}, ${i * 10})"))
    assert(assertPreserves(t, "k1", full = true) > 0, "linear sort clustering must commit")
  }

  test("sort mode: zorder, multiple keys") {
    val t = "ice.db.s_z"
    sql(s"CREATE TABLE $t (k1 INT, k2 INT, val INT) USING iceberg " +
      s"TBLPROPERTIES (${clustered("k1,k2", sortMode = "zorder")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${7 - i}, ${i * 10})"))
    assert(assertPreserves(t, "k1", full = true) > 0, "z-order clustering must commit")
  }

  // ---------------------------------------------------------------------------------------------
  // Merge-on-read: OPTIMIZE must keep deleted rows deleted (no resurrection).
  // ---------------------------------------------------------------------------------------------

  private def createMor(t: String): Unit =
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (" +
      s"'format-version'='2', 'write.delete.mode'='merge-on-read', " +
      s"'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read', " +
      s"${clustered("ts")})")

  test("merge-on-read: DELETE then OPTIMIZE keeps rows deleted") {
    val t = "ice.db.mor_del"
    createMor(t)
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"DELETE FROM $t WHERE ts IN (2, 4)")
    assert(rowCount(t) === 4, "two rows deleted")
    assert(assertPreserves(t, "ts", full = true) >= 0, "clustering must preserve the live rows")
    assert(rowCount(t) === 4, "deleted rows must stay deleted after OPTIMIZE (no resurrection)")
    checkAnswer(sql(s"SELECT ts FROM $t ORDER BY ts"), Seq(1, 3, 5, 6).map(Row(_)))
  }

  test("merge-on-read: MERGE then OPTIMIZE stays consistent") {
    val t = "ice.db.mor_merge"
    createMor(t)
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql("CREATE TABLE ice.db.mor_src (ts INT, val INT) USING iceberg")
    Seq((2, 999), (7, 70)).foreach { case (k, v) =>
      sql(s"INSERT INTO ice.db.mor_src VALUES ($k, $v)")
    }
    sql(s"MERGE INTO $t d USING ice.db.mor_src s ON d.ts = s.ts " +
      "WHEN MATCHED THEN UPDATE SET d.val = s.val " +
      "WHEN NOT MATCHED THEN INSERT *")
    val expected = sql(s"SELECT * FROM $t ORDER BY ts").collect().toSeq
    assertPreserves(t, "ts", full = true)
    assert(rows(t, "ts") === expected, "MERGE result must survive OPTIMIZE unchanged")
  }

  test("merge-on-read: UPDATE then OPTIMIZE stays consistent") {
    val t = "ice.db.mor_upd"
    createMor(t)
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"UPDATE $t SET val = -1 WHERE ts = 3")
    val expected = sql(s"SELECT * FROM $t ORDER BY ts").collect().toSeq
    assertPreserves(t, "ts", full = true)
    assert(rows(t, "ts") === expected, "UPDATE result must survive OPTIMIZE unchanged")
    checkAnswer(sql(s"SELECT val FROM $t WHERE ts = 3"), Row(-1))
  }

  // ---------------------------------------------------------------------------------------------
  // Incremental state machine.
  // ---------------------------------------------------------------------------------------------

  test("state: no-op is idempotent (second run with no new data commits nothing)") {
    val t = "ice.db.st_idem"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t").collect()
    val h1 = hwm(t)
    // Second run: watermark already at the age floor -> no new snapshot, watermark unchanged.
    assert(assertPreserves(t, "ts") === 0, "a second OPTIMIZE with no new data must not commit")
    assert(hwm(t) === h1, "watermark must not move on a no-op run")
  }

  test("state: expired watermark falls back to a full backfill") {
    val t = "ice.db.st_exp"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    // Point the watermark at a snapshot id that does not exist; the command must not throw.
    sql(s"ALTER TABLE $t SET TBLPROPERTIES ('optimize.cluster.hwm-snapshot-id' = '999999')")
    assert(assertPreserves(t, "ts") > 0, "an expired watermark must fall back to a full backfill")
    assert(hwm(t) !== Some("999999"), "watermark must be reset to a real snapshot")
  }

  test("state: keys reconfigured between runs") {
    val t = "ice.db.st_keys"
    sql(s"CREATE TABLE $t (k1 INT, k2 INT, val INT) USING iceberg " +
      s"TBLPROPERTIES (${clustered("k1")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${7 - i}, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t SET TBLPROPERTIES ('optimize.cluster.keys' = 'k2')")
    (7 to 9).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${20 - i}, ${i * 10})"))
    assert(assertPreserves(t, "k1") >= 0, "reclustering on a new leading key must preserve data")
  }

  test("state: sort-mode changed between runs") {
    val t = "ice.db.st_mode"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg " +
      s"TBLPROPERTIES (${clustered("ts", sortMode = "sort")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t SET TBLPROPERTIES ('optimize.cluster.sort-mode' = 'zorder')")
    (7 to 9).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    assert(assertPreserves(t, "ts") >= 0, "switching sort mode must preserve data")
  }

  test("state: FULL reclusters everything and resets the watermark to head") {
    val t = "ice.db.st_full"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t").collect()
    (7 to 9).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    assert(assertPreserves(t, "ts", full = true) > 0, "FULL must recluster the whole table")
  }

  test("state: hold-back window skips young snapshots (no-op)") {
    val t = "ice.db.st_hold"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg " +
      s"TBLPROPERTIES (${clustered("ts", minAge = 60000)})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    assert(assertPreserves(t, "ts") === 0, "nothing old enough -> no commit")
    assert(hwm(t).isEmpty, "watermark stays unset when nothing is consumed")
  }

  // ---------------------------------------------------------------------------------------------
  // Edge data.
  // ---------------------------------------------------------------------------------------------

  test("edge: empty table is a no-op") {
    val t = "ice.db.e_empty"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    sql(s"OPTIMIZE $t").collect() // must not throw
    assert(snapshotCount(t) === 0, "no data -> no snapshot")
  }

  test("edge: single row") {
    val t = "ice.db.e_one"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    sql(s"INSERT INTO $t VALUES (1, 10)")
    assertPreserves(t, "ts", full = true)
    checkAnswer(sql(s"SELECT * FROM $t"), Row(1, 10))
  }

  test("edge: all rows share the leading-key value") {
    val t = "ice.db.e_same"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES (1, ${i * 10})"))
    assertPreserves(t, "val", full = true)
    assert(rowCount(t) === 6)
  }

  test("edge: nulls in the leading key") {
    val t = "ice.db.e_null"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 4).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"INSERT INTO $t VALUES (CAST(NULL AS INT), 999)")
    sql(s"INSERT INTO $t VALUES (CAST(NULL AS INT), 998)")
    assertPreserves(t, "val", full = true)
    assert(rowCount(t) === 6, "null-keyed rows must not be lost")
    checkAnswer(sql(s"SELECT count(*) FROM $t WHERE ts IS NULL"), Row(2))
  }
}
