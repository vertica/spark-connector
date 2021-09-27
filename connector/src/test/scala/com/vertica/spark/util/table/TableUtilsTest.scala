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

package com.vertica.spark.util.table

import java.sql.{ResultSet, SQLException, SQLSyntaxErrorException}

import com.vertica.spark.config.{LogProvider, TableName}
import com.vertica.spark.datasource.jdbc.{JdbcLayerParam, JdbcLayerStringParam}
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.{ConnectionError, JobStatusCreateError, SyntaxError, TableCheckError}
import com.vertica.spark.util.schema.{SchemaTools, SchemaToolsInterface}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class TableUtilsTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {

  private val strlen = 1024

  private def checkResult(result: ConnectorResult[Unit]): Unit= {
    result match {
      case Left(errors) => fail(errors.getFullContext)
      case Right(_) => ()
    }
  }

  private def getTableResultSet(exists: Boolean) : ResultSet = {
    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true)
    (resultSet.getInt(_: Int)).expects(1).returning(if(exists) 1 else 0)
    (resultSet.close _).expects().returning()

    resultSet
  }

  private def getBooleanResultSet(exists: Boolean) : ResultSet = {
    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true)
    (resultSet.getBoolean(_: String)).expects(*).returning(exists)
    (resultSet.close _).expects().returning()

    resultSet
  }

  private def getBooleanThrowResultSet() : ResultSet = {
    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true)
    (resultSet.getBoolean(_: String)).expects(*).throwing(new java.sql.SQLSyntaxErrorException("SQL err"))
    (resultSet.close _).expects().returning()

    resultSet
  }

  it should "Return true if table exists" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      "select count(*) from v_catalog.tables where table_schema ILIKE ? and table_name ILIKE ?",
      Seq(JdbcLayerStringParam("public"), JdbcLayerStringParam("dummy"))
    ).returning(Right(getTableResultSet(exists = true)))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaTools, jdbcLayerInterface)

    utils.tableExists(TableName(tablename, None)) match {
      case Left(errors) => fail(errors.getFullContext)
      case Right(v) => assert(v)
    }
  }

  it should "Return false if table does not exist" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      *,
      *
    ).returning(Right(getTableResultSet(exists = false)))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaTools, jdbcLayerInterface)

    utils.tableExists(TableName(tablename, None)) match {
      case Left(errors) => fail(errors.getFullContext)
      case Right(v) => assert(!v)
    }
  }

  it should "Pass on error from JDBC" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      *,
      *
    ).returning(Left(ConnectionError(new Exception())))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaTools, jdbcLayerInterface)

    utils.tableExists(TableName(tablename, None)) match {
      case Left(err) => assert(err.getUnderlyingError match {
        case TableCheckError(_) => true
        case _ => false
      })
      case Right(_) => fail
    }
  }

  it should "Return true if view exists" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      "select count(*) from views where table_schema ILIKE ? and table_name ILIKE ?",
      Seq(JdbcLayerStringParam("public"), JdbcLayerStringParam("dummy"))
    ).returning(Right(getTableResultSet(exists = true)))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaTools, jdbcLayerInterface)

    utils.viewExists(TableName(tablename, None)) match {
      case Left(errors) => fail(errors.getFullContext)
      case Right(v) => assert(v)
    }
  }

  it should "Return false if view does not exist" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      *,
      *
    ).returning(Right(getTableResultSet(exists = false)))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaTools, jdbcLayerInterface)

    utils.viewExists(TableName(tablename, None)) match {
      case Left(errors) => fail(errors.getFullContext)
      case Right(v) => assert(!v)
    }
  }

  it should "Return true if temp table exists" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      "select is_temp_table as t from v_catalog.tables where table_name=? and table_schema=?",
      Seq(JdbcLayerStringParam("dummy"), JdbcLayerStringParam("public"))
    ).returning(Right(getBooleanResultSet(exists = true)))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaTools, jdbcLayerInterface)

    utils.tempTableExists(TableName(tablename, None)) match {
      case Left(errors) => fail(errors.getFullContext)
      case Right(v) => assert(v)
    }
  }

  it should "Return error if resultset throws" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      "select is_temp_table as t from v_catalog.tables where table_name=? and table_schema=?",
      Seq(JdbcLayerStringParam("dummy"), JdbcLayerStringParam("public"))
    ).returning(Right(getBooleanThrowResultSet()))
    (jdbcLayerInterface.handleJDBCException _).expects(*).returning(SyntaxError(new SQLSyntaxErrorException("SQL Error")))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaTools, jdbcLayerInterface)

    utils.tempTableExists(TableName(tablename, None)) match {
      case Left(errors) => errors.getUnderlyingError.isInstanceOf[SyntaxError]
      case Right(_) => fail
    }
  }

  it should "Create a table building statement" in {
    val tablename = "dummy"

    val schema = new StructType(Array(StructField("col1", IntegerType)))

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.execute _).expects(
      "CREATE table \"dummy\" (\"col1\" INTEGER) INCLUDE SCHEMA PRIVILEGES ",
      *
    ).returning(Right(()))

    val schemaTools = new SchemaTools()

    val utils = new TableUtils(schemaTools, jdbcLayerInterface)

    utils.createTable(TableName(tablename, None), None, schema, strlen) match {
      case Left(errors) => fail(errors.getFullContext)
      case Right(_) => ()
    }
  }

  it should "Create an external table building statement" in {
    val tablename = "dummy"

    val schema = new StructType(Array(StructField("col1", IntegerType)))

    val url = "hdfs://test:8020/fff"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.execute _).expects(
      "CREATE EXTERNAL table \"dummy\" (\"col1\" INTEGER) AS COPY FROM '" + url + "' PARQUET",
      *
    ).returning(Right(()))

    val schemaTools = new SchemaTools()

    val utils = new TableUtils(schemaTools, jdbcLayerInterface)

    utils.createExternalTable(TableName(tablename, None), None, schema, strlen, url) match {
      case Left(errors) => fail(errors.getFullContext)
      case Right(_) => ()
    }
  }

  it should "Create a table using provided statement" in {
    val tablename = "dummy"

    val schema = new StructType(Array(StructField("col1", IntegerType)))

    val stmt = "CREATE table dummy(col1 INTEGER)"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.execute _).expects(
      stmt,
      *
    ).returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaToolsInterface, jdbcLayerInterface)

    utils.createTable(TableName(tablename, None), Some(stmt), schema, strlen) match {
      case Left(errors) => fail(errors.getFullContext)
      case Right(_) => ()
    }
  }

  it should "Create the job status table if it doesn't exist" in {
    val tablename = "dummy"
    val user = "user"
    val sessionId = "session_id"

    val jobStatusTableName = "S2V_JOB_STATUS" + "_USER_" + user.toUpperCase

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      "select count(*) from v_catalog.tables where table_schema ILIKE ? and table_name ILIKE ?",
      Seq(JdbcLayerStringParam("public"), JdbcLayerStringParam(jobStatusTableName))
    ).returning(Right(getTableResultSet(exists = false)))
    (jdbcLayerInterface.execute _).expects("CREATE TABLE IF NOT EXISTS \"public\".\"S2V_JOB_STATUS_USER_USER\"(target_table_schema VARCHAR(128), target_table_name VARCHAR(128), save_mode VARCHAR(128), job_name VARCHAR(256), start_time TIMESTAMPTZ, all_done BOOLEAN NOT NULL, success BOOLEAN NOT NULL, percent_failed_rows DOUBLE PRECISION)", *).returning(Right(()))
    (jdbcLayerInterface.execute _).expects(*,*).returning(Right(()))
    (jdbcLayerInterface.execute _).expects(*,*).returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaToolsInterface, jdbcLayerInterface)

    checkResult(utils.createAndInitJobStatusTable(TableName(tablename, None), user, sessionId, "OVERWRITE"))
  }

  it should "Add initial entry to job status table" in {
    val tablename = "dummy"
    val user = "user"
    val sessionId = "session_id"

    val jobStatusTableName = "S2V_JOB_STATUS" + "_USER_" + user.toUpperCase

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      *, *
    ).returning(Right(getTableResultSet(exists = false)))
    (jdbcLayerInterface.execute _).expects(*,*).returning(Right(()))
    (jdbcLayerInterface.execute _).expects(*,*).returning(Right(()))
    (jdbcLayerInterface.execute _).expects(where { (stmt: String, params: Seq[JdbcLayerParam]) =>
      stmt.startsWith("INSERT into \"public\".\"S2V_JOB_STATUS_USER_USER\" VALUES ('public','dummy','APPEND','session_id',")
    }).returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaToolsInterface, jdbcLayerInterface)

    checkResult(utils.createAndInitJobStatusTable(TableName(tablename, None), user, sessionId, "APPEND"))
  }

  it should "Pass on error from job status init" in {
    val tablename = "dummy"
    val user = "user"
    val sessionId = "session_id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      *,*
    ).returning(Right(getTableResultSet(exists = false)))
    (jdbcLayerInterface.execute _).expects(*,*).returning(Left(ConnectionError(new Exception())))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaToolsInterface, jdbcLayerInterface)

    utils.createAndInitJobStatusTable(TableName(tablename, None), user, sessionId, "OVERWRITE") match {
      case Right(_) => fail
      case Left(errors) => assert(errors.getUnderlyingError match {
        case JobStatusCreateError(_) => true
        case _ => false
      })
    }
  }

  it should "Update entry in job status table" in {
    val tablename = "dummy"
    val user = "user"
    val sessionId = "session_id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects("UPDATE \"public\".\"S2V_JOB_STATUS_USER_USER\" SET all_done=true,success=true,percent_failed_rows=0.1 WHERE job_name='session_id' AND all_done=false", *).returning(Right(1))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaToolsInterface, jdbcLayerInterface)

    checkResult(utils.updateJobStatusTable(TableName(tablename,None), user, 0.1, sessionId, success = true))
  }

  it should "Update entry in job status table with failure" in {
    val tablename = "dummy"
    val user = "user"
    val sessionId = "session_id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects("UPDATE \"public\".\"S2V_JOB_STATUS_USER_USER\" SET all_done=true,success=false,percent_failed_rows=0.2 WHERE job_name='session_id' AND all_done=false", *).returning(Right(1))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaToolsInterface, jdbcLayerInterface)

    checkResult(utils.updateJobStatusTable(TableName(tablename,None), user, 0.2, sessionId, success = false))
  }

  it should "Drop a table" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.execute _).expects("DROP TABLE IF EXISTS \"dummy\"", *).returning(Right())

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(schemaToolsInterface, jdbcLayerInterface)

    checkResult(utils.dropTable(TableName(tablename, None)))
  }
}
