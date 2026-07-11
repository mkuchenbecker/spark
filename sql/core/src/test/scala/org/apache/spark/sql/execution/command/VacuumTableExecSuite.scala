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

import java.util.Collections

import scala.collection.mutable

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.procedures.{BoundProcedure, ProcedureParameter, UnboundProcedure}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * End-to-end execution test for the VACUUM command. Registers recording
 * `system.expire_snapshots` / `system.remove_orphan_files` procedures on an
 * in-memory catalog and runs real `VACUUM` SQL, verifying the command dispatches
 * the expected `CALL`s in order.
 */
class VacuumTableExecSuite extends QueryTest with SharedSparkSession with BeforeAndAfter {

  private val invocations = mutable.ArrayBuffer[String]()

  before {
    spark.conf.set("spark.sql.catalog.cat", classOf[InMemoryCatalog].getName)
    val catalog =
      spark.sessionState.catalogManager.catalog("cat").asInstanceOf[InMemoryCatalog]
    catalog.createProcedure(
      Identifier.of(Array("system"), "expire_snapshots"),
      new RecordingProcedure("expire_snapshots"))
    catalog.createProcedure(
      Identifier.of(Array("system"), "remove_orphan_files"),
      new RecordingProcedure("remove_orphan_files"))
    invocations.clear()
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf("spark.sql.catalog.cat")
  }

  test("VACUUM runs snapshot expiration only by default") {
    sql("VACUUM cat.db.t")
    assert(invocations.toSeq === Seq("expire_snapshots:db.t"))
  }

  test("VACUUM OFD runs expiration then orphan-file deletion, in order") {
    sql("VACUUM cat.db.t OFD")
    assert(invocations.toSeq === Seq("expire_snapshots:db.t", "remove_orphan_files:db.t"))
  }

  test("VACUUM OFD RETAIN invokes both procedures") {
    sql("VACUUM cat.db.t OFD RETAIN 24 HOURS")
    assert(invocations.toSeq === Seq("expire_snapshots:db.t", "remove_orphan_files:db.t"))
  }

  /** Procedure double that records the `table` argument it was called with. */
  private class RecordingProcedure(procName: String)
    extends UnboundProcedure with BoundProcedure {

    override def name: String = procName
    override def description: String = s"recording $procName"
    override def bind(inputType: StructType): BoundProcedure = this
    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter.in("table", DataTypes.StringType).build(),
      ProcedureParameter.in("older_than", DataTypes.TimestampType)
        .defaultValue("current_timestamp()").build())

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      invocations += s"$procName:${input.getString(0)}"
      Collections.emptyIterator
    }
  }
}
