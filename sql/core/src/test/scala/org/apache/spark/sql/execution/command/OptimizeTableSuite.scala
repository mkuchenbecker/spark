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

  test("clusterCall: single quotes in the where clause are escaped for embedding") {
    val stmt = clusterCall("cat", "db.t", "sort", Seq("ts"),
      "`ts` <= TIMESTAMP '2026-01-01 00:00:00'", maxCommits = 5)
    assert(stmt.contains("where => '`ts` <= TIMESTAMP ''2026-01-01 00:00:00''', "))
    assert(stmt.contains("sort_order => 'ts', "))
    assert(stmt.contains("'partial-progress.max-commits', '5'"))
  }

  test("setHwmCall: advances the watermark table property") {
    assert(setHwmCall("cat", "db.t", 42L) ===
      "ALTER TABLE cat.db.t SET TBLPROPERTIES ('optimize.cluster.hwm-snapshot-id' = '42')")
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
