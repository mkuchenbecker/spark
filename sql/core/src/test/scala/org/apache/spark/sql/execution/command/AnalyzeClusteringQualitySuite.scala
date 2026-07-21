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
import org.apache.spark.sql.execution.command.OptimizeTableCommand.ClusterInterval

class AnalyzeClusteringQualitySuite extends SparkFunSuite {

  import AnalyzeClusteringQualityCommand._

  test("metricExpr: accesses the per-file readable metric, quoting the key") {
    assert(metricExpr("ts", "lower_bound") === "readable_metrics.ts.lower_bound")
    assert(metricExpr("my-col", "upper_bound") === "readable_metrics.`my-col`.upper_bound")
  }

  test("coveragePredicate: no intervals -> false") {
    assert(coveragePredicate("lo", "hi", Seq.empty, "INT") === "false")
  }

  test("coveragePredicate: bounded interval checks both sides, CAST to the key type") {
    val p = coveragePredicate("lo", "hi", Seq(ClusterInterval("c", "ts", "sort", Some("5"), "20")),
      "INT")
    assert(p === "((hi <= CAST('20' AS INT)) AND (lo > CAST('5' AS INT)))")
  }

  test("coveragePredicate: unbounded-below interval drops the lower check") {
    val p = coveragePredicate(
      "lo", "hi", Seq(ClusterInterval("c", "ts", "sort", None, "20")), "INT")
    assert(p === "((hi <= CAST('20' AS INT)) AND true)")
  }

  test("coveragePredicate: multiple intervals are ORed") {
    val p = coveragePredicate("lo", "hi", Seq(
      ClusterInterval("c", "ts", "sort", None, "10"),
      ClusterInterval("c", "ts", "sort", Some("10"), "20")), "INT")
    assert(p.contains(" OR "))
    assert(p.startsWith("((hi <= CAST('10' AS INT))"))
  }

  // Depth is computed with distributed SQL (see the windowed sweep in the command); its exact
  // avg/max math is pinned end-to-end in OptimizeClusteringIcebergSuite where bounds are known.
}
