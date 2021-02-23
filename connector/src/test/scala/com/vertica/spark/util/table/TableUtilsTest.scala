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

import java.sql.ResultSet

import ch.qos.logback.classic.Level
import com.vertica.spark.config.{LogProvider, TableName}
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ConnectorErrorType.{JobStatusCreateError, TableCheckError}
import com.vertica.spark.util.error.{ConnectorError, JDBCLayerError}
import com.vertica.spark.util.error.JdbcErrorType.ConnectionError
import com.vertica.spark.util.schema.SchemaToolsInterface
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class TableUtilsTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {

  private val logProvider = LogProvider(Level.ERROR)
  private val strlen = 1024

  private def checkResult(eith: Either[ConnectorError, Unit]): Unit= {
    eith match {
      case Left(err) => fail(err.msg)
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

  it should "Return true if table exists" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      "select count(*) from v_catalog.tables where table_schema ILIKE 'public' and table_name ILIKE 'dummy'"
    ).returning(Right(getTableResultSet(exists = true)))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaTools, jdbcLayerInterface)

    utils.tableExists(TableName(tablename, None)) match {
      case Left(err) => fail(err.msg)
      case Right(v) => assert(v)
    }
  }

  it should "Return false if table does not exist" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      "select count(*) from v_catalog.tables where table_schema ILIKE 'public' and table_name ILIKE 'dummy'"
    ).returning(Right(getTableResultSet(exists = false)))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaTools, jdbcLayerInterface)

    utils.tableExists(TableName(tablename, None)) match {
      case Left(err) => fail(err.msg)
      case Right(v) => assert(!v)
    }
  }

  it should "Pass on error from JDBC" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      "select count(*) from v_catalog.tables where table_schema ILIKE 'public' and table_name ILIKE 'dummy'"
    ).returning(Left(JDBCLayerError(ConnectionError)))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaTools, jdbcLayerInterface)

    utils.tableExists(TableName(tablename, None)) match {
      case Left(err) => assert(err.err == TableCheckError)
      case Right(v) => fail
    }
  }

  it should "Return true if view exists" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      "select count(*) from views where table_schema ILIKE 'public' and table_name ILIKE 'dummy'"
    ).returning(Right(getTableResultSet(exists = true)))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaTools, jdbcLayerInterface)

    utils.viewExists(TableName(tablename, None)) match {
      case Left(err) => fail(err.msg)
      case Right(v) => assert(v)
    }
  }

  it should "Return false if view does not exist" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      "select count(*) from views where table_schema ILIKE 'public' and table_name ILIKE 'dummy'"
    ).returning(Right(getTableResultSet(exists = false)))

    val schemaTools = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaTools, jdbcLayerInterface)

    utils.viewExists(TableName(tablename, None)) match {
      case Left(err) => fail(err.msg)
      case Right(v) => assert(!v)
    }
  }

  it should "Create a table building statement" in {
    val tablename = "dummy"

    val schema = new StructType(Array(StructField("col1", IntegerType)))

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.execute _).expects(
      "CREATE table \"dummy\" (\"col1\" INTEGER)  INCLUDE SCHEMA PRIVILEGES "
    ).returning(Right(()))

    val schemaTools = mock[SchemaToolsInterface]
    (schemaTools.getVerticaTypeFromSparkType _).expects(IntegerType, strlen).returning(Right("INTEGER"))

    val utils = new TableUtils(logProvider, schemaTools, jdbcLayerInterface)

    utils.createTable(TableName(tablename, None), None, schema, strlen) match {
      case Left(err) => fail(err.msg)
      case Right(v) => ()
    }
  }

  it should "Create a table using provided statement" in {
    val tablename = "dummy"

    val schema = new StructType(Array(StructField("col1", IntegerType)))

    val stmt = "CREATE table dummy(col1 INTEGER)"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.execute _).expects(
      stmt
    ).returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaToolsInterface, jdbcLayerInterface)

    utils.createTable(TableName(tablename, None), Some(stmt), schema, strlen) match {
      case Left(err) => fail(err.msg)
      case Right(v) => ()
    }
  }

  it should "Create the job status table if it doesn't exist" in {
    val tablename = "dummy"
    val user = "user"
    val sessionId = "session_id"

    val jobStatusTableName = "S2V_JOB_STATUS" + "_USER_" + user.toUpperCase

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      "select count(*) from v_catalog.tables where table_schema ILIKE 'public' and table_name ILIKE '" + jobStatusTableName + "'"
    ).returning(Right(getTableResultSet(exists = false)))
    (jdbcLayerInterface.execute _).expects("CREATE TABLE IF NOT EXISTS \"public\".\"S2V_JOB_STATUS_USER_USER\"(target_table_schema VARCHAR(128), target_table_name VARCHAR(128), save_mode VARCHAR(128), job_name VARCHAR(256), start_time TIMESTAMPTZ, all_done BOOLEAN NOT NULL, success BOOLEAN NOT NULL, percent_failed_rows DOUBLE PRECISION)").returning(Right(()))
    (jdbcLayerInterface.execute _).expects(*).returning(Right(()))
    (jdbcLayerInterface.execute _).expects(*).returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaToolsInterface, jdbcLayerInterface)

    checkResult(utils.createAndInitJobStatusTable(TableName(tablename, None), user, sessionId))
  }

  it should "Add initial entry to job status table" in {
    val tablename = "dummy"
    val user = "user"
    val sessionId = "session_id"

    val jobStatusTableName = "S2V_JOB_STATUS" + "_USER_" + user.toUpperCase

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      *
    ).returning(Right(getTableResultSet(exists = false)))
    (jdbcLayerInterface.execute _).expects(*).returning(Right(()))
    (jdbcLayerInterface.execute _).expects(*).returning(Right(()))
    (jdbcLayerInterface.execute _).expects(where { stmt: String =>
      stmt.startsWith("INSERT into public.S2V_JOB_STATUS_USER_USER VALUES ('public','dummy','OVERWRITE','session_id',")
    }).returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaToolsInterface, jdbcLayerInterface)

    checkResult(utils.createAndInitJobStatusTable(TableName(tablename, None), user, sessionId))
  }

  it should "Pass on error from job status init" in {
    val tablename = "dummy"
    val user = "user"
    val sessionId = "session_id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(
      *
    ).returning(Right(getTableResultSet(exists = false)))
    (jdbcLayerInterface.execute _).expects(*).returning(Left(JDBCLayerError(ConnectionError)))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaToolsInterface, jdbcLayerInterface)

    utils.createAndInitJobStatusTable(TableName(tablename, None), user, sessionId) match {
      case Right(_) => fail
      case Left(err) => assert(err.err == JobStatusCreateError)
    }
  }

  it should "Update entry in job status table" in {
    val tablename = "dummy"
    val user = "user"
    val sessionId = "session_id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects("UPDATE public.S2V_JOB_STATUS_USER_USER SET all_done=true,success=true,percent_failed_rows=0.1 WHERE job_name='session_id' AND all_done=false").returning(Right(1))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaToolsInterface, jdbcLayerInterface)

    checkResult(utils.updateJobStatusTable(TableName(tablename,None), user, 0.1, sessionId, success = true))
  }

  it should "Update entry in job status table with failure" in {
    val tablename = "dummy"
    val user = "user"
    val sessionId = "session_id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects("UPDATE public.S2V_JOB_STATUS_USER_USER SET all_done=true,success=false,percent_failed_rows=0.2 WHERE job_name='session_id' AND all_done=false").returning(Right(1))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaToolsInterface, jdbcLayerInterface)

    checkResult(utils.updateJobStatusTable(TableName(tablename,None), user, 0.2, sessionId, success = false))
  }

  it should "Drop a table" in {
    val tablename = "dummy"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.execute _).expects("DROP TABLE IF EXISTS dummy").returning(Right(1))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val utils = new TableUtils(logProvider, schemaToolsInterface, jdbcLayerInterface)

    checkResult(utils.dropTable(TableName(tablename, None)))
  }
}
