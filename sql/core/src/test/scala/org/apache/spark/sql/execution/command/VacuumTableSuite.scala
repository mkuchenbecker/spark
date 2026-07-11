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

class VacuumTableSuite extends SparkFunSuite {

  test("callStatements: snapshot expiration only, default retention") {
    assert(
      VacuumTableCommand.callStatements(
        "cat", Seq("db", "t"), removeOrphanFiles = false, retainHours = None) ===
      Seq("CALL cat.system.expire_snapshots(table => 'db.t')"))
  }

  test("callStatements: OFD appends remove_orphan_files after expiration") {
    assert(
      VacuumTableCommand.callStatements(
        "cat", Seq("db", "t"), removeOrphanFiles = true, retainHours = None) ===
      Seq(
        "CALL cat.system.expire_snapshots(table => 'db.t')",
        "CALL cat.system.remove_orphan_files(table => 'db.t')"))
  }

  test("callStatements: RETAIN bounds both operations via older_than") {
    assert(
      VacuumTableCommand.callStatements(
        "cat", Seq("db", "t"), removeOrphanFiles = true, retainHours = Some(168)) ===
      Seq(
        "CALL cat.system.expire_snapshots(table => 'db.t'," +
          " older_than => current_timestamp() - INTERVAL 168 HOURS)",
        "CALL cat.system.remove_orphan_files(table => 'db.t'," +
          " older_than => current_timestamp() - INTERVAL 168 HOURS)"))
  }

  test("callStatements: quotes the catalog identifier when required") {
    assert(
      VacuumTableCommand.callStatements(
        "my-cat", Seq("t"), removeOrphanFiles = false, retainHours = None) ===
      Seq("CALL `my-cat`.system.expire_snapshots(table => 't')"))
  }
}
