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

class OptimizeTableSuite extends SparkFunSuite {

  import OptimizeTableCommand._

  test("parseClusterConfig: resolves defaults and parses typed values") {
    val empty = parseClusterConfig(Map.empty)
    assert(empty.keys.isEmpty)
    assert(empty.sortMode === DEFAULT_SORT_MODE)
    assert(empty.minAgeMinutes === DEFAULT_MIN_SNAPSHOT_AGE_MINUTES)
    assert(empty.maxCommits === DEFAULT_MAX_COMMITS)
    assert(empty.hwm.isEmpty)
    assert(empty.state.isEmpty)

    val cfg = parseClusterConfig(Map(
      KEYS_PROP -> " ts , uid ",
      SORT_MODE_PROP -> "sort",
      MIN_SNAPSHOT_AGE_PROP -> "5",
      MAX_COMMITS_PROP -> "3",
      HWM_PROP -> "42",
      STATE_PROP -> """[{"config":"c1","keys":"ts","mode":"sort","upper":"20"}]"""))
    assert(cfg.keys === Seq("ts", "uid")) // trimmed, comma-split
    assert(cfg.sortMode === "sort")
    assert(cfg.minAgeMinutes === 5L)
    assert(cfg.maxCommits === 3L)
    assert(cfg.hwm === Some(42L))
    assert(cfg.state === Seq(ClusterInterval("c1", "ts", "sort", None, "20")))
  }

  test("configId: stable across whitespace, changes on key or mode change") {
    assert(configId(Seq("ts", "uid"), "zorder") === configId(Seq(" ts ", " uid "), "ZORDER"))
    assert(configId(Seq("ts", "uid"), "zorder") !== configId(Seq("ts"), "zorder"))
    assert(configId(Seq("ts"), "zorder") !== configId(Seq("ts"), "sort"))
  }

  test("state: parses the serialized interval JSON, with and without the optional lower bound") {
    val json = """[{"config":"c1","keys":"ts","mode":"sort","lower":"10","upper":"20"},""" +
      """{"config":"c2","keys":"ts,uid","mode":"zorder","upper":"2026-01-06 00:00:00"}]"""
    assert(parseState(json) === Seq(
      ClusterInterval("c1", "ts", "sort", Some("10"), "20"),
      ClusterInterval("c2", "ts,uid", "zorder", None, "2026-01-06 00:00:00")))
  }

  test("state: empty or absent input parses as no state") {
    assert(parseState("") === Seq.empty)
    assert(parseState(null) === Seq.empty)
  }

  test("state: malformed input fails loudly with clear-the-property guidance") {
    val e = intercept[IllegalStateException](parseState("not json"))
    assert(e.getMessage.contains(STATE_PROP))
    assert(e.getMessage.contains("UNSET TBLPROPERTIES"))
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
}
