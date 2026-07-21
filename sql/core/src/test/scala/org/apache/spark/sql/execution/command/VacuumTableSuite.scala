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

import java.time.{Instant, ZoneId}

import org.apache.spark.SparkFunSuite

class VacuumTableSuite extends SparkFunSuite {

  private val ref = Instant.parse("2026-01-02T03:04:05Z")

  test("olderThan: subtracts the retention window and renders millisecond precision") {
    assert(VacuumTableCommand.olderThan(ref, 0, ZoneId.of("UTC")) === "2026-01-02 03:04:05.000")
    assert(VacuumTableCommand.olderThan(ref, 24, ZoneId.of("UTC")) === "2026-01-01 03:04:05.000")
  }

  test("olderThan: renders the cutoff in the given time zone") {
    // UTC-08:00 shifts the same instant back by 8 hours in wall-clock terms.
    assert(
      VacuumTableCommand.olderThan(ref, 0, ZoneId.of("America/Los_Angeles")) ===
        "2026-01-01 19:04:05.000")
  }
}
