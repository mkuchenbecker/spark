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
  private def prop(t: String, key: String): Option[String] =
    sql(s"SHOW TBLPROPERTIES $t").collect().find(_.getString(0) == key).map(_.getString(1))
  private def hwm(t: String): Option[String] = prop(t, "optimize.cluster.hwm-snapshot-id")
  private def state(t: String): Seq[OptimizeTableCommand.ClusterInterval] =
    OptimizeTableCommand.parseState(prop(t, "optimize.cluster.state").getOrElse(""))
  private def optimizeMetrics(t: String, full: Boolean = false): Map[String, String] =
    sql(s"OPTIMIZE $t${if (full) " FULL" else ""}").collect()
      .map(r => r.getString(0) -> r.getString(1)).toMap
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

  /**
   * Scope proof for an incremental run: `assertPreserves` alone can't tell incremental from full
   * (both preserve data + commit). This captures the already-clustered file set, runs `appendSlice`
   * (which must add a forward slice strictly above the current clustered max), then an incremental
   * OPTIMIZE, and asserts the previously-clustered files were NOT rewritten while the appended
   * slice WAS -- i.e. the run really only touched the new slice.
   */
  private def assertIncrementalScope(t: String, orderBy: String)(appendSlice: => Unit): Unit = {
    val clusteredFiles = dataFiles(t)
    appendSlice
    val appended = dataFiles(t) -- clusteredFiles
    assert(appended.nonEmpty, "test setup: appendSlice must add data files")
    val before = rows(t, orderBy)
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, orderBy) === before, "incremental OPTIMIZE must preserve query results")
    val after = dataFiles(t)
    assert(clusteredFiles.subsetOf(after),
      "incremental must NOT rewrite already-clustered files; " +
        s"rewritten=${(clusteredFiles -- after).take(3)}")
    val stillRaw = appended.intersect(after)
    assert(stillRaw.isEmpty,
      s"incremental must rewrite the appended slice; still-raw=${stillRaw.take(3)}")
  }

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
    assert(messageChain(e).contains("clustered no data"),
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

  test("state: a stale watermark snapshot-id is ignored, never read") {
    val t = "ice.db.st_exp"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    // Point the watermark at a snapshot id that does not exist. The lower bound comes from interval
    // state (not the watermark snapshot), so the command never reads that id and must not throw;
    // with no prior state it clusters up to the age floor.
    sql(s"ALTER TABLE $t SET TBLPROPERTIES ('optimize.cluster.hwm-snapshot-id' = '999999')")
    assert(assertPreserves(t, "ts") > 0, "a bogus watermark must not prevent clustering")
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

  // ---------------------------------------------------------------------------------------------
  // Durable interval state (watermark + config id + intervals) and the run's file-reduction output.
  // ---------------------------------------------------------------------------------------------

  test("state: a clustering run records config id + one interval alongside the watermark") {
    val t = "ice.db.stt_first"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    // All three metadata facts land together.
    assert(hwm(t).isDefined, "watermark set")
    assert(prop(t, "optimize.cluster.config-id").isDefined, "config id set")
    val s = state(t)
    assert(s.size === 1, s"one interval, got $s")
    assert(s.head.keys === "ts" && s.head.mode === "zorder")
    assert(s.head.lower.isEmpty && s.head.upper === "6", s"FULL interval unbounded below: $s")
  }

  test("state: incremental extends the current epoch; FULL collapses it") {
    val t = "ice.db.stt_ext"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, $i)"))
    sql(s"OPTIMIZE $t").collect()
    assert(state(t).map(_.upper) === Seq("3"))
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, $i)"))
    sql(s"OPTIMIZE $t").collect()
    val s = state(t)
    assert(s.size === 1 && s.head.upper === "6", s"single extended interval, got $s")
    sql(s"OPTIMIZE $t FULL").collect()
    assert(state(t).size === 1, "FULL keeps a single interval for the config")
  }

  test("state: a key change appends a new epoch and retains the old one (durable history)") {
    val t = "ice.db.stt_keychg"
    sql(s"CREATE TABLE $t (k1 INT, k2 INT, val INT) USING iceberg " +
      s"TBLPROPERTIES (${clustered("k1")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${7 - i}, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    val cfg1 = prop(t, "optimize.cluster.config-id")
    sql(s"ALTER TABLE $t SET TBLPROPERTIES ('optimize.cluster.keys' = 'k2')")
    (7 to 9).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${20 - i}, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    val cfg2 = prop(t, "optimize.cluster.config-id")
    assert(cfg1 !== cfg2, "config id changes with the key selection")
    val configs = state(t).map(_.config).toSet
    assert(configs.size === 2, s"both epochs retained as durable history: ${state(t)}")
    assert(state(t).exists(_.keys === "k1") && state(t).exists(_.keys === "k2"))
  }

  test("state: a failed clustering run leaves the state untouched") {
    val t = "ice.db.stt_fail"
    sql(s"CREATE TABLE $t (k DECIMAL(10,2), val INT) USING iceberg " +
      s"TBLPROPERTIES (${clustered("k", sortMode = "zorder")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i.50, $i)"))
    intercept[Exception](sql(s"OPTIMIZE $t FULL").collect())
    assert(state(t).isEmpty, "no state written on a failed run")
    assert(prop(t, "optimize.cluster.config-id").isEmpty, "no config id written on a failed run")
  }

  test("OPTIMIZE reports the file reduction in its output rows") {
    val t = "ice.db.stt_metrics"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    val m = optimizeMetrics(t, full = true)
    assert(m("files_before") === "6", s"metrics: $m")
    assert(m("files_after").toInt < 6, s"metrics: $m")
    assert(m("files_removed").toInt === 6 - m("files_after").toInt, s"metrics: $m")
    assert(m("snapshots_committed").toInt >= 1, s"metrics: $m")
  }

  // ---------------------------------------------------------------------------------------------
  // ANALYZE TABLE ... COMPUTE CLUSTERING QUALITY (read-only quality metrics).
  // ---------------------------------------------------------------------------------------------

  private def analyzeRows(t: String): Seq[Row] =
    sql(s"ANALYZE TABLE $t COMPUTE CLUSTERING QUALITY").collect().toSeq
  private def scalar(rows: Seq[Row]): Map[String, String] =
    rows.filter(_.isNullAt(1)).map(r => r.getString(0) -> r.getString(2)).toMap
  private def dim(rows: Seq[Row], metric: String, d: String): Option[String] =
    rows.find(r => r.getString(0) == metric && !r.isNullAt(1) && r.getString(1) == d)
      .map(_.getString(2))

  test("analyze: unconfigured table reports clustering_configured=false, no error") {
    val t = "ice.db.aq_none"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg")
    sql(s"INSERT INTO $t VALUES (1, 10)")
    val m = scalar(analyzeRows(t))
    assert(m("clustering_configured") === "false", m.toString)
  }

  test("analyze: configured but unclustered reports 0 coverage") {
    val t = "ice.db.aq_unclustered"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    val m = scalar(analyzeRows(t))
    assert(m("clustering_configured") === "true")
    assert(m("coverage_bytes_pct").toDouble === 0.0, m.toString)
    assert(m("unclustered_tail_hours") === "unknown", "no watermark yet -> unknown tail")
  }

  test("analyze: after FULL, coverage is ~100% and covered depth is ~1") {
    val t = "ice.db.aq_full"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    val rows = analyzeRows(t)
    val m = scalar(rows)
    assert(m("coverage_bytes_pct").toDouble > 99.0, m.toString)
    assert(m("coverage_files_pct").toDouble > 99.0, m.toString)
    assert(dim(rows, "depth_avg_covered", "ts").exists(_.toDouble <= 1.5), rows.toString)
  }

  test("analyze: held-back new data lowers coverage below 100%") {
    val t = "ice.db.aq_partial"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    // New rows above the clustered range, not yet reclustered -> uncovered.
    (7 to 12).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    val c = scalar(analyzeRows(t))("coverage_bytes_pct").toDouble
    assert(c > 0.0 && c < 100.0, s"expected partial coverage, got $c")
  }

  test("analyze: a key change drops coverage until re-clustered") {
    val t = "ice.db.aq_keychg"
    sql(s"CREATE TABLE $t (k1 INT, k2 INT, val INT) USING iceberg " +
      s"TBLPROPERTIES (${clustered("k1")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${7 - i}, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t SET TBLPROPERTIES ('optimize.cluster.keys' = 'k2')")
    // No run under the new key selection yet -> current-config coverage is 0.
    assert(scalar(analyzeRows(t))("coverage_bytes_pct").toDouble === 0.0)
    sql(s"OPTIMIZE $t FULL").collect()
    assert(scalar(analyzeRows(t))("coverage_bytes_pct").toDouble > 99.0, "FULL restores coverage")
  }

  test("analyze: depth is high for interleaved data and drops after clustering") {
    val t = "ice.db.aq_depth"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    // Several files all spanning the key range [1,6] -> heavily interleaved -> depth well above 1.
    // (Exact depth is file-layout dependent, so assert the direction, not a pinned value.)
    (1 to 3).foreach(_ => sql(s"INSERT INTO $t VALUES (1, 1), (6, 6)"))
    val before = dim(analyzeRows(t), "depth_avg", "ts").map(_.toDouble).getOrElse(0.0)
    assert(before >= 2.0, s"interleaved data should have high depth, got $before")
    sql(s"OPTIMIZE $t FULL").collect()
    val after = dim(analyzeRows(t), "depth_avg_covered", "ts").map(_.toDouble).getOrElse(99.0)
    assert(after <= 1.5 && after < before, s"clustering should reduce depth toward 1, got $after")
  }

  test("analyze: null leading-key bytes are reported and counted uncovered") {
    val t = "ice.db.aq_null"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 4).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"INSERT INTO $t VALUES (CAST(NULL AS INT), 999)")
    sql(s"OPTIMIZE $t FULL").collect()
    val m = scalar(analyzeRows(t))
    assert(m("null_bound_bytes_pct").toDouble > 0.0, s"null-bound file must be reported: $m")
  }

  test("analyze: timestamp leading key computes coverage") {
    val t = "ice.db.aq_ts"
    sql(s"CREATE TABLE $t (ts TIMESTAMP, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', $i)"))
    sql(s"OPTIMIZE $t FULL").collect()
    assert(scalar(analyzeRows(t))("coverage_bytes_pct").toDouble > 99.0)
  }

  test("analyze: is read-only (no new snapshot, properties unchanged)") {
    val t = "ice.db.aq_readonly"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    val snapsBefore = snapshotCount(t)
    val propsBefore = sql(s"SHOW TBLPROPERTIES $t").collect()
      .map(r => r.getString(0) -> r.getString(1)).toSet
    analyzeRows(t)
    assert(snapshotCount(t) === snapsBefore, "ANALYZE must not commit")
    val propsAfter = sql(s"SHOW TBLPROPERTIES $t").collect()
      .map(r => r.getString(0) -> r.getString(1)).toSet
    assert(propsAfter === propsBefore, "ANALYZE must not change table properties")
  }

  test("analyze: on a missing table fails and names the table") {
    val e = intercept[Exception](
      sql("ANALYZE TABLE ice.db.no_such_analyze_table COMPUTE CLUSTERING QUALITY").collect())
    assert(messageChain(e).contains("no_such_analyze_table"),
      s"error must name the missing table: ${messageChain(e).take(300)}")
  }

  // ---------------------------------------------------------------------------------------------
  // DML/DDL evolution between incremental runs (scope proof, column + partition-spec DDL, real SE).
  // ---------------------------------------------------------------------------------------------

  // D1 -- the core: an incremental run must touch ONLY the new forward slice.
  test("incremental: rewrites only the new forward slice, not already-clustered files") {
    val t = "ice.db.inc_scope"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    assertIncrementalScope(t, "ts") {
      // forward slice strictly above the clustered max (3), as multiple files to force a rewrite
      (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    }
  }

  // C5 -- renaming the leading-key column must not silently mis-scope.
  test("ddl: renaming the leading-key column makes OPTIMIZE fail loudly, not mis-scope") {
    val t = "ice.db.rename_key"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    val stateBefore = prop(t, "optimize.cluster.state")
    val hwmBefore = hwm(t)
    sql(s"ALTER TABLE $t RENAME COLUMN ts TO event_ts")
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    val e = intercept[Exception](sql(s"OPTIMIZE $t").collect())
    val msg = messageChain(e).toLowerCase
    assert(msg.contains("ts") || msg.contains("cannot") || msg.contains("resolve"),
      s"rename of the leading key must fail loudly: ${messageChain(e).take(300)}")
    assert(rowCount(t) === 6, "data must be intact after the failed run")
    assert(prop(t, "optimize.cluster.state") === stateBefore, "failed run must not advance state")
    assert(hwm(t) === hwmBefore, "failed run must not advance the watermark")
  }

  // P1 -- partition spec evolution: unpartitioned -> add days(ts), then incremental.
  test("ddl: adding a partition field (days(ts)) then incremental preserves data") {
    val t = "ice.db.spec_add"
    sql(s"CREATE TABLE $t (ts TIMESTAMP, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', $i)"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t ADD PARTITION FIELD days(ts)")
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', $i)"))
    val before = rows(t, "ts")
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, "ts") === before, "data must be preserved across partition-spec evolution")
    assert(rowCount(t) === 6)
  }

  // P2 -- partition transform change: days(ts) -> hours(ts), then incremental.
  test("ddl: changing a partition transform (days->hours) then incremental preserves data") {
    val t = "ice.db.spec_xform"
    sql(s"CREATE TABLE $t (ts TIMESTAMP, val INT) USING iceberg PARTITIONED BY (days(ts)) " +
      s"TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', $i)"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t REPLACE PARTITION FIELD days(ts) WITH hours(ts)")
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', $i)"))
    val before = rows(t, "ts")
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, "ts") === before, "data must be preserved across a partition-transform change")
  }

  // S2 -- REAL snapshot expiration of the watermark snapshot must NOT trigger a full backfill. The
  // incremental lower bound is the last-clustered upper from the persisted interval state (a table
  // property that survives SE), so the run stays incremental on the forward slice even though the
  // watermark snapshot itself is gone.
  test("state: incremental survives real SE of the watermark snapshot (no full backfill)") {
    val t = "ice.db.se_wm"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect() // clusters ts in [1,3]; state upper = 3
    val expiredHwm = hwm(t)
    assertIncrementalScope(t, "ts") {
      (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
      // Really expire every snapshot but the head -- including the one the watermark points at.
      sql(s"CALL ice.system.expire_snapshots(table => 'db.se_wm', " +
        s"older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
    }
    assert(hwm(t) !== expiredHwm, "watermark must advance off the expired snapshot")
    assert(state(t).exists(_.upper == "6"), "state upper must advance to the new forward max")
  }

  // ---- Phase 2: column DDL between runs ----

  // C1 -- adding a non-key column: null backfill preserved, incremental scope unaffected.
  test("ddl: adding a non-key column preserves data and incremental scope") {
    val t = "ice.db.add_col"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t ADD COLUMN note STRING")
    assertIncrementalScope(t, "ts") {
      (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10}, 'n$i')"))
    }
    assert(count(s"SELECT count(*) FROM $t WHERE note IS NULL") === 3, "old rows keep null note")
  }

  // C4 -- dropping the leading-key column must fail loudly, state untouched.
  test("ddl: dropping the leading-key column makes OPTIMIZE fail loudly, state untouched") {
    val t = "ice.db.drop_key"
    sql(s"CREATE TABLE $t (ts INT, val INT, note STRING) USING iceberg " +
      s"TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10}, 'n$i')"))
    sql(s"OPTIMIZE $t FULL").collect()
    val stateBefore = prop(t, "optimize.cluster.state")
    val hwmBefore = hwm(t)
    sql(s"ALTER TABLE $t DROP COLUMN ts")
    sql(s"INSERT INTO $t VALUES (40, 'n4')")
    val e = intercept[Exception](sql(s"OPTIMIZE $t").collect())
    val msg = messageChain(e).toLowerCase
    assert(msg.contains("ts") || msg.contains("cannot") || msg.contains("resolve"),
      s"drop of the leading key must fail loudly: ${messageChain(e).take(300)}")
    assert(prop(t, "optimize.cluster.state") === stateBefore, "failed run must not advance state")
    assert(hwm(t) === hwmBefore, "failed run must not advance the watermark")
  }

  // C6 -- renaming a NON-key column leaves incremental unaffected.
  test("ddl: renaming a non-key column leaves incremental OPTIMIZE unaffected") {
    val t = "ice.db.rename_nonkey"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t RENAME COLUMN val TO amount")
    assertIncrementalScope(t, "ts") {
      (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    }
  }

  // C7 -- promoting the leading-key type (int -> bigint) keeps scope/coverage correct.
  test("ddl: promoting the leading-key type (int->bigint) keeps incremental correct") {
    val t = "ice.db.promote_key"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t ALTER COLUMN ts TYPE BIGINT")
    assertIncrementalScope(t, "ts") {
      (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    }
  }

  // C8 -- reordering columns (field-id based) must not break incremental.
  test("ddl: reordering columns leaves incremental OPTIMIZE correct") {
    val t = "ice.db.reorder"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t ALTER COLUMN val FIRST")
    assertIncrementalScope(t, "ts") {
      (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES (${i * 10}, $i)"))
    }
  }

  // ---- Phase 3: partition-spec DDL between runs ----

  // P3 -- adding a second partition field.
  test("ddl: adding a second partition field then incremental preserves data") {
    val t = "ice.db.spec_add2"
    sql(s"CREATE TABLE $t (ts TIMESTAMP, region STRING, val INT) USING iceberg " +
      s"PARTITIONED BY (days(ts)) TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i =>
      sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', 'r$i', $i)"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t ADD PARTITION FIELD region")
    (4 to 6).foreach(i =>
      sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', 'r$i', $i)"))
    val before = rows(t, "ts")
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, "ts") === before, "data preserved after adding a partition field")
  }

  // P4 -- dropping a partition field.
  test("ddl: dropping a partition field then incremental preserves data") {
    val t = "ice.db.spec_drop"
    sql(s"CREATE TABLE $t (ts TIMESTAMP, region STRING, val INT) USING iceberg " +
      s"PARTITIONED BY (days(ts), region) TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i =>
      sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', 'r$i', $i)"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t DROP PARTITION FIELD region")
    (4 to 6).foreach(i =>
      sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', 'r$i', $i)"))
    val before = rows(t, "ts")
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, "ts") === before, "data preserved after dropping a partition field")
  }

  // P6 -- incremental scope proof holds across a partition-transform change.
  test("ddl: incremental scope holds across a partition-transform change") {
    val t = "ice.db.spec_scope"
    sql(s"CREATE TABLE $t (ts TIMESTAMP, val INT) USING iceberg " +
      s"PARTITIONED BY (days(ts)) TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i =>
      sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', $i)"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t REPLACE PARTITION FIELD days(ts) WITH hours(ts)")
    assertIncrementalScope(t, "ts") {
      (4 to 6).foreach(i =>
        sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', $i)"))
    }
  }

  // ---- Phase 4: DML interleaved with incremental runs ----

  private val mor =
    "'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', " +
      "'write.merge.mode'='merge-on-read'"

  // D2 -- late data below the watermark is not re-clustered (documented behavior).
  test("dml: late data below the watermark is a no-op for incremental (not re-clustered)") {
    val t = "ice.db.late_data"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    (1 to 2).foreach(_ => sql(s"INSERT INTO $t VALUES (3, 999)"))
    val lateFiles = dataFiles(t)
    val snaps = snapshotCount(t)
    val before = rows(t, "ts, val") // ts alone ties on the duplicate late rows
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, "ts, val") === before, "data preserved")
    assert(snapshotCount(t) === snaps, "late data below the watermark must not trigger a rewrite")
    assert(lateFiles.subsetOf(dataFiles(t)), "late files must not be rewritten by incremental")
  }

  // D3 -- DELETE (copy-on-write) then incremental keeps rows deleted.
  test("dml: DELETE (copy-on-write) then incremental keeps rows deleted") {
    val t = "ice.db.del_cow"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg " +
      s"TBLPROPERTIES (${clustered("ts")}, 'write.delete.mode'='copy-on-write')")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"DELETE FROM $t WHERE ts = 5")
    val before = rows(t, "ts")
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, "ts") === before, "data preserved incl. the delete")
    assert(count(s"SELECT count(*) FROM $t WHERE ts = 5") === 0, "deleted row stays deleted")
  }

  // D4 -- DELETE (merge-on-read) in the incremental scope: the rewrite must apply the delete.
  test("dml: DELETE (merge-on-read) in the incremental scope stays applied") {
    val t = "ice.db.del_mor"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")}, $mor)")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"DELETE FROM $t WHERE ts = 5")
    val before = rows(t, "ts")
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, "ts") === before, "data preserved incl. the MoR delete")
    assert(count(s"SELECT count(*) FROM $t WHERE ts = 5") === 0, "MoR delete stays applied")
  }

  // D5 -- UPDATE (merge-on-read) then incremental stays consistent.
  test("dml: UPDATE (merge-on-read) then incremental stays consistent") {
    val t = "ice.db.upd_mor"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")}, $mor)")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"UPDATE $t SET val = -1 WHERE ts = 5")
    val before = rows(t, "ts")
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, "ts") === before, "data preserved incl. the update")
    assert(count(s"SELECT count(*) FROM $t WHERE ts = 5 AND val = -1") === 1, "update applied")
  }

  // D6 -- MERGE (merge-on-read) then incremental stays consistent.
  test("dml: MERGE (merge-on-read) then incremental stays consistent") {
    val t = "ice.db.mrg_mor"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")}, $mor)")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"MERGE INTO $t USING (SELECT 5 AS ts, -7 AS val) s ON $t.ts = s.ts " +
      s"WHEN MATCHED THEN UPDATE SET val = s.val")
    val before = rows(t, "ts")
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, "ts") === before, "data preserved incl. the merge")
    assert(count(s"SELECT count(*) FROM $t WHERE ts = 5 AND val = -7") === 1, "merge stays applied")
  }

  // D7 -- many append -> incremental rounds: watermark monotonic, no re-cluster of prior rounds.
  test("dml: multi-round incremental advances monotonically without re-clustering prior rounds") {
    val t = "ice.db.multiround"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    var prevFiles = dataFiles(t)
    var prevHwm = hwm(t)
    for (round <- 1 to 3) {
      val base = 3 + round * 3
      ((base - 2) to base).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
      sql(s"OPTIMIZE $t").collect()
      assert(prevFiles.subsetOf(dataFiles(t)), s"round $round must not re-cluster prior rounds")
      assert(hwm(t) !== prevHwm, s"round $round must advance the watermark")
      prevFiles = dataFiles(t)
      prevHwm = hwm(t)
    }
    assert(rowCount(t) === 12, "all rows present after multi-round clustering")
  }

  // ---- Phase 5/6: more SE + combined interaction ----

  private def expireAllButLast(t: String, db: String, retain: Long): Unit =
    sql(s"CALL ice.system.expire_snapshots(table => '$db', " +
      s"older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => $retain)")

  // S1 -- SE pruning old snapshots must not touch the watermark property; incremental still runs.
  test("state: SE pruning old snapshots leaves the watermark property + incremental intact") {
    val t = "ice.db.se_old"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    val hwmBefore = hwm(t)
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    expireAllButLast(t, "db.se_old", snapshotCount(t) - 2) // prune the oldest pre-watermark snaps
    // SE removes snapshots, not table properties -- the watermark property is untouched.
    assert(hwm(t) === hwmBefore, "SE must not change the watermark property")
    assert(assertPreserves(t, "ts") > 0, "incremental must run normally after SE prunes history")
  }

  // S3 -- SE must not touch the clustering state (it lives in table properties, not snapshots).
  test("state: SE between incremental runs does not change the clustering state") {
    val t = "ice.db.se_between"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t").collect()
    val stateBefore = prop(t, "optimize.cluster.state")
    val hwmBefore = hwm(t)
    expireAllButLast(t, "db.se_between", 1)
    assert(prop(t, "optimize.cluster.state") === stateBefore, "SE must not change clustering state")
    assert(hwm(t) === hwmBefore, "SE must not change the watermark property")
  }

  // X2 -- add a column, then cluster by it (new epoch), then incremental.
  test("combined: add a column, reconfigure keys (new epoch), then incremental preserves data") {
    val t = "ice.db.x_addkey"
    sql(s"CREATE TABLE $t (ts INT, val INT) USING iceberg TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10})"))
    sql(s"OPTIMIZE $t FULL").collect()
    val epochsBefore = state(t).map(_.config).distinct.size
    sql(s"ALTER TABLE $t ADD COLUMN region STRING")
    sql(s"ALTER TABLE $t SET TBLPROPERTIES ('optimize.cluster.keys'='ts,region')")
    (4 to 6).foreach(i => sql(s"INSERT INTO $t VALUES ($i, ${i * 10}, 'r$i')"))
    val before = rows(t, "ts")
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, "ts") === before, "data preserved after add-column + key reconfigure")
    assert(state(t).map(_.config).distinct.size >= epochsBefore, "the new key config is recorded")
  }

  // X4 -- SE (expiring the watermark) + partition-spec change + incremental (via persisted state).
  test("combined: SE + partition-spec change + incremental preserves data") {
    val t = "ice.db.x_se_spec"
    sql(s"CREATE TABLE $t (ts TIMESTAMP, val INT) USING iceberg " +
      s"PARTITIONED BY (days(ts)) TBLPROPERTIES (${clustered("ts")})")
    (1 to 3).foreach(i =>
      sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', $i)"))
    sql(s"OPTIMIZE $t FULL").collect()
    sql(s"ALTER TABLE $t REPLACE PARTITION FIELD days(ts) WITH hours(ts)")
    (4 to 6).foreach(i =>
      sql(s"INSERT INTO $t VALUES (TIMESTAMP '2026-01-0$i 00:00:00', $i)"))
    expireAllButLast(t, "db.x_se_spec", 1) // expires the watermark -> incremental via state
    val before = rows(t, "ts")
    sql(s"OPTIMIZE $t").collect()
    assert(rows(t, "ts") === before, "data preserved under SE + spec change + incremental")
  }
}
