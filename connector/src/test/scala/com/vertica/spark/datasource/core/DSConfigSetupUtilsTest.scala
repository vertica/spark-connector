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

package com.vertica.spark.datasource.core

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyChain, ValidatedNec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import ch.qos.logback.classic.Level
import com.vertica.spark.config.TableName
import org.scalamock.scalatest.MockFactory
import com.vertica.spark.util.error._
import org.scalactic.{Equality, TolerantNumerics}

class DSConfigSetupUtilsTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  def getResultOrAssert[ResultType](validation : ValidatedNec[_,ResultType]): ResultType = {
    validation match {
      case Invalid(_) => fail
      case Valid(result) => result
    }
  }

  def getErrorOrAssert[ErrorType](validation : ValidatedNec[ErrorType,_]): NonEmptyChain[ErrorType] = {
    validation match {
      case Invalid(errors) => errors
      case Valid(_) => fail
    }
  }

  implicit val floatEquality: Equality[Float] = TolerantNumerics.tolerantFloatEquality(0.01f)

  it should "parse the logging level" in {
    var opts = Map("logging_level" -> "ERROR")
    var level = getResultOrAssert[Level](DSConfigSetupUtils.getLogLevel(opts))
    assert(level == Level.ERROR)

    opts = Map("logging_level" -> "DEBUG")
    level = getResultOrAssert[Level](DSConfigSetupUtils.getLogLevel(opts))
    assert(level == Level.DEBUG)

    opts = Map("logging_level" -> "WARNING")
    level = getResultOrAssert[Level](DSConfigSetupUtils.getLogLevel(opts))
    assert(level == Level.WARN)

    opts = Map("logging_level" -> "INFO")
    level = getResultOrAssert[Level](DSConfigSetupUtils.getLogLevel(opts))
    assert(level == Level.INFO)
  }

  it should "default to ERROR logging level" in {
    val opts = Map[String, String]()
    val level = getResultOrAssert[Level](DSConfigSetupUtils.getLogLevel(opts))
    assert(level == Level.ERROR)
  }

  it should "error given incorrect logging_level param" in {
    val opts = Map("logging_level" -> "OTHER")
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getLogLevel(opts))
    assert(err.toNonEmptyList.head == InvalidLoggingLevel())
  }

  it should "parse the host name" in {
    val opts = Map("host" -> "1.1.1.1")
    val host = getResultOrAssert[String](DSConfigSetupUtils.getHost(opts))
    assert(host == "1.1.1.1")
  }

  it should "fail with missing host name" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getHost(opts))
    assert(err.toNonEmptyList.head == HostMissingError())
  }

  it should "parse the port" in {
    val opts = Map("port" -> "1234")
    val port = getResultOrAssert[Int](DSConfigSetupUtils.getPort(opts))
    assert(port == 1234)
  }

  it should "defaults to port 5543" in {
    val opts = Map[String, String]()
    val port = getResultOrAssert[Int](DSConfigSetupUtils.getPort(opts))
    assert(port == 5433)
  }

  it should "error with invalid port input" in {
    var opts = Map("port" -> "abc123")
    var err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getPort(opts))
    assert(err.toNonEmptyList.head == InvalidPortError())

    opts = Map("port" -> "1.1")
    err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getPort(opts))
    assert(err.toNonEmptyList.head == InvalidPortError())
  }

  it should "parse the failed row tolerance" in {
    val opts = Map("failed_rows_percent_tolerance" -> "0.05")
    val tol = getResultOrAssert[Float](DSConfigSetupUtils.getFailedRowsPercentTolerance(opts))
    assert(tol === 0.05f)
  }

  it should "default to zero failed row tolerance" in {
    val opts = Map[String, String]()
    val tol = getResultOrAssert[Float](DSConfigSetupUtils.getFailedRowsPercentTolerance(opts))
    assert(tol === 0.00f)
  }

  it should "error on invalid failed row tolerance" in {
    val opts = Map("failed_rows_percent_tolerance" -> "1.5")
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getFailedRowsPercentTolerance(opts))
    assert(err.toNonEmptyList.head == InvalidFailedRowsTolerance())
  }

  it should "parse the db name" in {
    val opts = Map("db" -> "testdb")
    val db = getResultOrAssert[String](DSConfigSetupUtils.getDb(opts))
    assert(db == "testdb")
  }

  it should "fail with missing db name" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getDb(opts))
    assert(err.toNonEmptyList.head == DbMissingError())
  }

  it should "parse the username" in {
    val opts = Map("user" -> "testuser")
    val user = getResultOrAssert[String](DSConfigSetupUtils.getUser(opts))
    assert(user == "testuser")
  }

  it should "fail with missing username" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getUser(opts))
    assert(err.toNonEmptyList.head == UserMissingError())
  }

  it should "parse the partition count" in {
    val opts = Map("num_partitions" -> "5")
    val pCount = getResultOrAssert[Option[Int]](DSConfigSetupUtils.getPartitionCount(opts))
    assert(pCount.get == 5)
  }

  it should "fail on invalid partition count" in {
    val opts = Map("num_partitions" -> "asdf")
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getPartitionCount(opts))
    assert(err.toNonEmptyList.head == InvalidPartitionCountError())
  }

  it should "parse the table name" in {
    val opts = Map("table" -> "tbl")
    val table = getResultOrAssert[String](DSConfigSetupUtils.getTablename(opts))
    assert(table == "tbl")
  }

  it should "parse the db schema" in {
    val opts = Map("dbschema" -> "test")
    val schema = getResultOrAssert[Option[String]](DSConfigSetupUtils.getDbSchema(opts))
    assert(schema.get == "test")
  }

  it should "default to no schema" in {
    val opts = Map[String, String]()
    val schema = getResultOrAssert[Option[String]](DSConfigSetupUtils.getDbSchema(opts))
    schema match {
      case Some(_) => fail
      case None => ()
    }
  }

  it should "parse full table name with schema" in {
    val opts = Map("dbschema" -> "test", "table" -> "table")
    val schema = getResultOrAssert[TableName](DSConfigSetupUtils.validateAndGetFullTableName(opts))
    assert(schema.getFullTableName == "\"test\".\"table\"")
  }

  it should "fail with missing table name" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getTablename(opts))
    assert(err.toNonEmptyList.head == TablenameMissingError())
  }

  it should "parse the password" in {
    val opts = Map("password" -> "pass")
    val pass = getResultOrAssert[String](DSConfigSetupUtils.getPassword(opts))
    assert(pass == "pass")
  }

  it should "fail with missing password" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getPassword(opts))
    assert(err.toNonEmptyList.head == PasswordMissingError())
  }

  it should "parse the staging filesystem url" in {
    val opts = Map[String, String]("staging_fs_url" -> "hdfs://test:8020/tmp/test")
    val url = getResultOrAssert [String](DSConfigSetupUtils.getStagingFsUrl(opts))
    assert(url == "hdfs://test:8020/tmp/test")
  }

  it should "fail with missing staging filesystem url" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getStagingFsUrl(opts))
    assert(err.toNonEmptyList.head == StagingFsUrlMissingError())
  }

  it should "parse the strlen" in {
    val opts = Map("strlen" -> "1234")
    val strlen = getResultOrAssert[Long](DSConfigSetupUtils.getStrLen(opts))
    assert(strlen == 1234)
  }

  it should "defaults to strlen 1024" in {
    val opts = Map[String, String]()
    val strlen = getResultOrAssert[Long](DSConfigSetupUtils.getStrLen(opts))
    assert(strlen == 1024)
  }

  it should "error with invalid strlen input" in {
    var opts = Map("strlen" -> "abc123")
    var err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getStrLen(opts))
    assert(err.toNonEmptyList.head == InvalidStrlenError())

    opts = Map("strlen" -> "1.1")
    err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getStrLen(opts))
    assert(err.toNonEmptyList.head == InvalidStrlenError())
  }

  it should "parse target table SQL" in {
    val stmt = "CREATE TABLE t1(col1 INTEGER);"
    val opts = Map("target_table_sql" -> stmt)

    val res = getResultOrAssert[Option[String]](DSConfigSetupUtils.getTargetTableSQL(opts))

    res match {
      case Some(str) => assert(str == stmt)
      case None => fail
    }
  }

  it should "parse custom column copy list" in {
    val stmt = "col1"
    val opts = Map("copy_column_list" -> stmt)

    val res = getResultOrAssert[Option[String]](DSConfigSetupUtils.getCopyColumnList(opts))

    res match {
      case Some(str) => assert(str == stmt)
      case None => fail
    }
  }
}
