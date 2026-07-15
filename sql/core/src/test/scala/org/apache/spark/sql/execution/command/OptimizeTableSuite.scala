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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.quoteIfNeeded

class OptimizeTableSuite extends SparkFunSuite {

  import OptimizeTableCommand._

  test("compactionCall: plain bin-pack rewrite when no clustering is configured") {
    assert(compactionCall("cat", "db.t") ===
      "CALL cat.system.rewrite_data_files(table => 'db.t')")
  }

  test("compactionCall: emits the (already-quoted) catalog identifier verbatim") {
    // The command quotes the resolved catalog first; this helper passes it through verbatim.
    assert(compactionCall(quoteIfNeeded("my-cat"), "t") ===
      "CALL `my-cat`.system.rewrite_data_files(table => 't')")
  }

  test("rewriteManifestsCall: manifest compaction") {
    assert(rewriteManifestsCall("cat", "db.t") ===
      "CALL cat.system.rewrite_manifests(table => 'db.t')")
  }

  test("sortOrderExpr: zorder wraps the keys; sort lists them") {
    assert(sortOrderExpr("zorder", Seq("k1", "k2")) === "zorder(k1, k2)")
    assert(sortOrderExpr("ZORDER", Seq("k1", "k2")) === "zorder(k1, k2)") // case-insensitive
    assert(sortOrderExpr("sort", Seq("k1", "k2")) === "k1, k2")
    assert(sortOrderExpr("zorder", Seq("my-col")) === "zorder(`my-col`)") // identifier quoting
  }

  test("clusterCall: scoped sort rewrite with partial progress and clustering options") {
    assert(
      clusterCall("cat", "db.t", "zorder", Seq("ts", "uid"), "`ts` <= 5", maxCommits = 10) ===
      "CALL cat.system.rewrite_data_files(" +
        "table => 'db.t', " +
        "strategy => 'sort', " +
        "sort_order => 'zorder(ts, uid)', " +
        "where => '`ts` <= 5', " +
        "options => map(" +
        "'min-input-files', '1', " +
        "'rewrite-all', 'true', " +
        "'use-starting-sequence-number', 'true', " +
        "'partial-progress.enabled', 'true', " +
        "'partial-progress.max-commits', '10'))")
  }

  test("clusterCall: quotes in the where clause survive as a Catalyst string literal") {
    val stmt = clusterCall("cat", "db.t", "sort", Seq("ts"),
      "`ts` <= TIMESTAMP '2026-01-01 00:00:00'", maxCommits = 5)
    // Rendered via Literal(...).sql, so inner quotes are backslash-escaped the way Spark re-parses.
    assert(stmt.contains("where => '`ts` <= TIMESTAMP \\'2026-01-01 00:00:00\\'', "), stmt)
    assert(stmt.contains("sort_order => 'ts', "))
    assert(stmt.contains("'partial-progress.max-commits', '5'"))
  }

  test("setClusterMetaCall: advances watermark, config id, and state in one statement") {
    val stmt = setClusterMetaCall("cat", "db.t", 42L, "abc123", """[{"config":"abc123"}]""")
    assert(stmt.startsWith("ALTER TABLE cat.db.t SET TBLPROPERTIES ("))
    assert(stmt.contains("'optimize.cluster.hwm-snapshot-id' = '42'"))
    assert(stmt.contains("'optimize.cluster.config-id' = 'abc123'"))
    // State JSON embedded as a Catalyst literal; JSON double-quotes are literal inside the
    // single-quoted SQL string (only single-quotes/backslashes would need escaping).
    assert(stmt.contains("""'optimize.cluster.state' = '[{"config":"abc123"}]'"""), stmt)
  }

  test("configId: stable across whitespace, changes on key or mode change") {
    assert(configId(Seq("ts", "uid"), "zorder") === configId(Seq(" ts ", " uid "), "ZORDER"))
    assert(configId(Seq("ts", "uid"), "zorder") !== configId(Seq("ts"), "zorder"))
    assert(configId(Seq("ts"), "zorder") !== configId(Seq("ts"), "sort"))
  }

  test("state: render/parse round-trips, including the optional lower bound") {
    val intervals = Seq(
      ClusterInterval("c1", "ts", "sort", Some("10"), "20"),
      ClusterInterval("c2", "ts,uid", "zorder", None, "2026-01-06 00:00:00"))
    assert(parseState(renderState(intervals)) === intervals)
  }

  test("state: malformed or empty input parses as no state") {
    assert(parseState("") === Seq.empty)
    assert(parseState(null) === Seq.empty)
    assert(parseState("not json") === Seq.empty)
  }

  test("advanceState: first run creates an interval") {
    assert(advanceState(Seq.empty, "c1", Seq("ts"), "sort", Some("5"), "10", full = false) ===
      Seq(ClusterInterval("c1", "ts", "sort", Some("5"), "10")))
  }

  test("advanceState: same-config incremental extends the upper, keeps the lower") {
    val s0 = Seq(ClusterInterval("c1", "ts", "sort", Some("5"), "10"))
    assert(advanceState(s0, "c1", Seq("ts"), "sort", Some("10"), "20", full = false) ===
      Seq(ClusterInterval("c1", "ts", "sort", Some("5"), "20")))
  }

  test("advanceState: FULL collapses the current config to one unbounded interval") {
    val s0 = Seq(ClusterInterval("c1", "ts", "sort", Some("5"), "20"))
    assert(advanceState(s0, "c1", Seq("ts"), "sort", None, "30", full = true) ===
      Seq(ClusterInterval("c1", "ts", "sort", None, "30")))
  }

  test("advanceState: a config change appends a new epoch and retains the old one") {
    val s0 = Seq(ClusterInterval("c1", "ts", "sort", None, "20"))
    val s1 = advanceState(s0, "c2", Seq("ts", "uid"), "zorder", Some("20"), "40", full = false)
    assert(s1 === Seq(
      ClusterInterval("c1", "ts", "sort", None, "20"),
      ClusterInterval("c2", "ts,uid", "zorder", Some("20"), "40")))
  }

  test("scopePredicate: incremental run bounds the leading key on both sides") {
    val p = scopePredicate("ts", Some(10), 20)
    assert(p.contains("ts > 10"), p)
    assert(p.contains("ts <= 20"), p)
    assert(p.contains("AND"), p)
  }

  test("scopePredicate: full / first run bounds only the upper side") {
    val p = scopePredicate("ts", None, 20)
    assert(p.contains("ts <= 20"), p)
    assert(!p.contains(">"), p)
    assert(!p.contains("AND"), p)
  }

  test("scopePredicate: string literal keys are quoted by Catalyst") {
    val p = scopePredicate("region", None, "us-west")
    assert(p.contains("'us-west'"), p)
  }
}
