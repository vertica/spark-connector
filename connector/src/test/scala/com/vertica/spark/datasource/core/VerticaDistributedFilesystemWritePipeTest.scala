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

import java.sql.ResultSet

import ch.qos.logback.classic.Level
import com.vertica.spark.config.{DistributedFilesystemWriteConfig, FileStoreConfig, JDBCConfig, TableName}
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ConnectorErrorType.{OpenWriteError, SchemaConversionError, TableCheckError}
import com.vertica.spark.util.error.JdbcErrorType.SyntaxError
import com.vertica.spark.util.error.{ConnectorError, JDBCLayerError, SchemaError}
import com.vertica.spark.util.error.SchemaErrorType.MissingConversionError
import com.vertica.spark.util.schema.SchemaToolsInterface
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class VerticaDistributedFilesystemWritePipeTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {
  private val tablename = TableName("dummy", None)
  private val jdbcConfig = JDBCConfig("1.1.1.1", 1234, "test", "test", "test", Level.ERROR)
  private val fileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/test", Level.ERROR)
  private val strlen = 1024

  private def checkResult(eith: Either[ConnectorError, Unit]): Unit= {
    eith match {
      case Left(err) => fail(err.msg)
      case Right(_) => ()
    }
  }

  // TODO: Change this based on write modes
  it should "Create a table if it doesn't exist" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None)

    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true).twice()
    (resultSet.getInt(_: Int)).expects(1).returning(0)
    (resultSet.getInt(_: Int)).expects(1).returning(1)

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects("select count(*) from v_catalog.tables where table_schema ILIKE 'public' and table_name ILIKE 'dummy'").returning(Right(resultSet)).twice()
    (jdbcLayerInterface.execute _).expects("CREATE table \"dummy\" (\"col1\" INTEGER)  INCLUDE SCHEMA PRIVILEGES ").returning(Right())

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getVerticaTypeFromSparkType _).expects(IntegerType, strlen).returning(Right("INTEGER"))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(*).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface , mock[SessionIdInterface])

    pipe.doPreWriteSteps() match {
      case Left(err) => fail(err.msg)
      case Right(_) => ()
    }
  }

  it should "Create a table using custom logic if it doesn't exist" in {
    val createTableStatement = "CREATE table dummy(col1 INTEGER);"

    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = Some(createTableStatement))

    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true).twice()
    (resultSet.getInt(_: Int)).expects(1).returning(0)
    (resultSet.getInt(_: Int)).expects(1).returning(1)

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects("select count(*) from v_catalog.tables where table_schema ILIKE 'public' and table_name ILIKE 'dummy'").returning(Right(resultSet)).twice()
    (jdbcLayerInterface.execute _).expects(createTableStatement).returning(Right())

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(*).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface , mock[SessionIdInterface])

    pipe.doPreWriteSteps() match {
      case Left(err) => fail(err.msg)
      case Right(_) => ()
    }
  }

  it should "Pass on errors from schema tools" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None)

    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true)
    (resultSet.getInt(_: Int)).expects(1).returning(0)

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects("select count(*) from v_catalog.tables where table_schema ILIKE 'public' and table_name ILIKE 'dummy'").returning(Right(resultSet))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getVerticaTypeFromSparkType _).expects(IntegerType, strlen).returning(Left(SchemaError(MissingConversionError)))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface , mock[SessionIdInterface])

    pipe.doPreWriteSteps() match {
      case Left(err) => assert(err.err == SchemaConversionError)
      case Right(_) => fail
    }
  }

  it should "Pass on errors from JDBC" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None)

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects("select count(*) from v_catalog.tables where table_schema ILIKE 'public' and table_name ILIKE 'dummy'").returning(Left(JDBCLayerError(SyntaxError)))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface , mock[SessionIdInterface])

    pipe.doPreWriteSteps() match {
      case Left(err) => assert(err.err == TableCheckError)
      case Right(_) => fail
    }
  }

  it should "Create the directory for exporting files" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None)

    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true).twice()
    (resultSet.getInt(_: Int)).expects(1).returning(1).twice()

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(*).returning(Right(resultSet)).twice()

    val schemaToolsInterface = mock[SchemaToolsInterface]

    // Directory w/ configured address is created
    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(fileStoreConfig.address).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface , mock[SessionIdInterface])

    pipe.doPreWriteSteps() match {
      case Left(err) => fail(err.msg)
      case Right(_) => ()
    }
  }

  it should "use filestore layer to write data" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None)

    val uniqueId = "unique-id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.close _).expects().returning()
    val schemaToolsInterface = mock[SchemaToolsInterface]

    val v1: Int = 1
    val v2: Float = 2.0f
    val v3: Float = 3.0f
    val dataBlock = DataBlock(List(InternalRow(v1,v2), InternalRow(v1,v3)))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.openWriteParquetFile _).expects(fileStoreConfig.address + "/" + uniqueId + ".parquet").returning(Right(()))
    (fileStoreLayerInterface.writeDataToParquetFile _).expects(dataBlock).returning(Right(()))
    (fileStoreLayerInterface.closeWriteParquetFile _).expects().returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface , mock[SessionIdInterface])

    checkResult(pipe.startPartitionWrite(uniqueId))
    checkResult(pipe.writeData(dataBlock))
    checkResult(pipe.endPartitionWrite())
  }

  it should "pass on errors from filestore layer on write" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None)

    val uniqueId = "unique-id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.openWriteParquetFile _).expects(*).returning(Left(ConnectorError(OpenWriteError)))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface , mock[SessionIdInterface])

    pipe.startPartitionWrite(uniqueId) match {
      case Right(_) => fail
      case Left(err) => assert(err.err == OpenWriteError)
    }
  }
}
