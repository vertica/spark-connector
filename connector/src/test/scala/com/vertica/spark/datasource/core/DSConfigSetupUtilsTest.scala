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
import com.vertica.spark.config.{JdbcAuth, KerberosAuth}
import com.vertica.spark.config.{TableName, TableQuery, TableSource, ValidColumnList, ValidFilePermissions}
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
    val user = DSConfigSetupUtils.getUser(opts)
    assert(user.contains("testuser"))
  }

  it should "returns empty username" in {
    val opts = Map[String, String]()
    val user = DSConfigSetupUtils.getUser(opts)
    assert(user.isEmpty)
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
    val table = DSConfigSetupUtils.getTablename(opts)
    assert(table.get == "tbl")
  }

  it should "parse the db schema" in {
    val opts = Map("dbschema" -> "test")
    val schema = DSConfigSetupUtils.getDbSchema(opts)
    assert(schema.get == "test")
  }

  it should "default to no schema" in {
    val opts = Map[String, String]()
    val schema = DSConfigSetupUtils.getDbSchema(opts)
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

  it should "get full table name of read" in {
    val opts = Map("dbschema" -> "test", "table" -> "table")
    val schema = getResultOrAssert[TableSource](DSConfigSetupUtils.validateAndGetTableSource(opts))
    assert(schema.isInstanceOf[TableName])
    assert(schema.asInstanceOf[TableName].getFullTableName == "\"test\".\"table\"")
  }

  it should "get table query" in {
    val q = "select * from abc where n > 5"
    val opts = Map("dbschema" -> "test", "query" -> q)
    val schema = getResultOrAssert[TableSource](DSConfigSetupUtils.validateAndGetTableSource(opts))
    assert(schema.isInstanceOf[TableQuery])
    assert(schema.asInstanceOf[TableQuery].query == q)
  }

  it should "error if no table or query specified" in {
    val opts = Map("dbschema" -> "test")
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.validateAndGetTableSource(opts))
    assert(err.toNonEmptyList.head == TableAndQueryMissingError())
  }

  it should "error on query on write" in {
    val q = "select * from abc where n > 5"
    val opts = Map("dbschema" -> "test", "query" -> q)
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.validateAndGetFullTableName(opts))
    assert(err.toNonEmptyList.head == QuerySpecifiedOnWriteError())
  }

  it should "parse the password" in {
    val opts = Map("password" -> "pass")
    val pass = DSConfigSetupUtils.getPassword(opts)
    assert(pass.contains("pass"))
  }

  it should "parse no password" in {
    val opts = Map[String, String]()
    val pass = DSConfigSetupUtils.getPassword(opts)
    assert(pass.isEmpty)
  }

  it should "parse kerberos options" in {
    val opts = Map(
      "host" -> "1.1.1.1",
      "port" -> "1234",
      "db" -> "testdb",
      "user" -> "user",
      "kerberos_service_name" -> "vertica",
      "kerberos_host_name" -> "vertica.example.com",
      "jaas_config_name" -> "Client"
    )

    val auth = getResultOrAssert[JdbcAuth](DSConfigSetupUtils.validateAndGetJDBCAuth(opts))

    assert(auth.isInstanceOf[KerberosAuth])
    assert(auth.asInstanceOf[KerberosAuth].kerberosServiceName == "vertica")
    assert(auth.asInstanceOf[KerberosAuth].kerberosHostname == "vertica.example.com")
    assert(auth.asInstanceOf[KerberosAuth].jaasConfigName == "Client")
  }

  it should "parse the staging filesystem url" in {
    val opts = Map[String, String]("staging_fs_url" -> "hdfs://test:8020/tmp/test")
    val url = getResultOrAssert[String](DSConfigSetupUtils.getStagingFsUrl(opts))
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

  it should "default to max file size 4096" in {
    val opts = Map[String, String]()
    val fileSize = getResultOrAssert[Int](DSConfigSetupUtils.getMaxFileSize(opts))
    assert(fileSize == 4096)
  }

  it should "default to max row group size 16" in {
    val opts = Map[String, String]()
    val fileSize = getResultOrAssert[Int](DSConfigSetupUtils.getMaxRowGroupSize(opts))
    assert(fileSize == 16)
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

    val res = getResultOrAssert[Option[ValidColumnList]](DSConfigSetupUtils.getCopyColumnList(opts))

    res match {
      case Some(list) => assert(list.toString == stmt)
      case None => fail
    }
  }

  it should "fail on unquoted semicolon" in {
    val stmt = "col1,fasd;,fda"
    val opts = Map("copy_column_list" -> stmt)

    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getCopyColumnList(opts))
    assert(err.toNonEmptyList.head.isInstanceOf[UnquotedSemiInColumns])
  }

  it should "don't fail on quoted semicolon" in {
    val stmt = "col1,\"fa;sd\",fda"
    val opts = Map("copy_column_list" -> stmt)

    val res = getResultOrAssert[Option[ValidColumnList]](DSConfigSetupUtils.getCopyColumnList(opts))

    res match {
      case Some(list) => assert(list.toString == stmt)
      case None => fail
    }
  }

  it should "parse file permissions" in {
    val stmt = "-rwxr-xr-x"
    val opts = Map("file_permissions" -> stmt)

    val res = getResultOrAssert[ValidFilePermissions](DSConfigSetupUtils.getFilePermissions(opts))

    assert(res.toString == stmt)
  }

  it should "parse octal file permissions" in {
    val stmt = "777"
    val opts = Map("file_permissions" -> stmt)

    val res = getResultOrAssert[ValidFilePermissions](DSConfigSetupUtils.getFilePermissions(opts))

    assert(res.toString == stmt)
  }

  it should "fail to parse invalid file permissions" in {
    val stmt = "777'; DROP TABLE test;"
    val opts = Map("file_permissions" -> stmt)

    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getFilePermissions(opts))

    assert(err.toNonEmptyList.head.isInstanceOf[InvalidFilePermissions])
  }

  it should "parse create_external_table option" in {
    val opts = Map("create_external_table" -> "true")
    val v = getResultOrAssert[Option[String]](DSConfigSetupUtils.getCreateExternalTable(opts))
    v match {
      case Some(value) => assert (value == "true")
      case _ => fail
    }
  }

  it should "parse second create_external_table option" in {
    val opts = Map("create_external_table" -> "existing")
    val v = getResultOrAssert[Option[String]](DSConfigSetupUtils.getCreateExternalTable(opts))
    v match {
      case Some(value) => assert (value == "existing")
      case _ => fail
    }
  }

  it should "error if create_external_table is not true/existing" in {
    val opts = Map("create_external_table" -> "asdf")
    val v = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getCreateExternalTable(opts))
    assert(v.toNonEmptyList.head.isInstanceOf[InvalidCreateExternalTableOption])
  }
}
