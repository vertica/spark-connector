// (c) Copyright [2020-2021] Micro Focus or one of its affiliates.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.vertica.spark.functests.endtoend

import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import com.vertica.spark.functests.TestUtils
import com.vertica.spark.util.error.{BinaryTypeNotSupported, ConnectorException, ErrorList}

import scala.util.{Failure, Success, Try}


/**
 * A few minimal tests for the json feature. Not intended to be comprehensive.
 * */
class BasicJsonReadTests(readOpts: Map[String, String], writeOpts: Map[String, String], jdbcConfig: JDBCConfig, fileStoreConfig: FileStoreConfig)
  extends EndToEnd(readOpts, writeOpts, jdbcConfig, fileStoreConfig) {

  private val jsonReadOpts = readOpts + ("json" -> "true")

  it should "read primitive types" in {
    val tableName1 = "dftest"
    val n = 1
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a int, b varchar, c float, d array[int])")

    TestUtils.populateTableBySQL(stmt, "insert into dftest values (1, 'heeelo', 3.2, array[3,5])", 10)

    val df =  spark.read.format("com.vertica.spark.datasource.VerticaSource")
      .options(jsonReadOpts + ("table" -> tableName1)).load()
    val result = Try {df.show()}
    result match {
      case Failure(exception) => fail("Expected to succeed", exception)
      case Success(_) =>
    }
    stmt.close()
    TestUtils.dropTable(conn, tableName1)
  }

  it should "error on binary types" in {
    val tableName = "dftest"
    val n = 1
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a binary, b varbinary, c array[binary], d array[varbinary], e long varbinary)")

    val df =  spark.read.format("com.vertica.spark.datasource.VerticaSource")
      .options(jsonReadOpts + ("table" -> tableName)).load()
    val result = Try{df.collect}
    result match {
      case Failure(exception) => exception match {
        case ConnectorException(error) => {
          assert(error.isInstanceOf[ErrorList])
          val errorList = error.asInstanceOf[ErrorList].errors.toList
          assert(errorList.forall(_.isInstanceOf[BinaryTypeNotSupported]))
          assert(errorList(0).asInstanceOf[BinaryTypeNotSupported].fieldName == "a")
          assert(errorList(1).asInstanceOf[BinaryTypeNotSupported].fieldName == "b")
          assert(errorList(2).asInstanceOf[BinaryTypeNotSupported].fieldName == "c")
          assert(errorList(3).asInstanceOf[BinaryTypeNotSupported].fieldName == "d")
          assert(errorList(4).asInstanceOf[BinaryTypeNotSupported].fieldName == "e")
        }
      }
      case Success(_) => fail("Expected to fail")
    }
    stmt.close()
    TestUtils.dropTable(conn, tableName)
  }
}
