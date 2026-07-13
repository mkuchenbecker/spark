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

  test("callStatements: data-file compaction only by default") {
    assert(
      OptimizeTableCommand.callStatements(
        "cat", Seq("db", "t"), rewriteManifests = false) ===
      Seq("CALL cat.system.rewrite_data_files(table => 'db.t')"))
  }

  test("callStatements: rewriteManifests appends rewrite_manifests after compaction") {
    assert(
      OptimizeTableCommand.callStatements(
        "cat", Seq("db", "t"), rewriteManifests = true) ===
      Seq(
        "CALL cat.system.rewrite_data_files(table => 'db.t')",
        "CALL cat.system.rewrite_manifests(table => 'db.t')"))
  }

  test("callStatements: quotes the catalog identifier when required") {
    assert(
      OptimizeTableCommand.callStatements(
        "my-cat", Seq("t"), rewriteManifests = false) ===
      Seq("CALL `my-cat`.system.rewrite_data_files(table => 't')"))
  }
}
