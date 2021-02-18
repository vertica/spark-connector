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
import com.vertica.spark.util.error.ConnectorErrorType.{CommitError, FaultToleranceTestFail, OpenWriteError, SchemaConversionError, TableCheckError, ViewExistsError}
import com.vertica.spark.util.error.JdbcErrorType.SyntaxError
import com.vertica.spark.util.error.{ConnectorError, JDBCLayerError}
import com.vertica.spark.util.schema.SchemaToolsInterface
import com.vertica.spark.util.table.TableUtilsInterface
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

  private def getCountTableResultSet(count: Int = 0) : ResultSet = {
    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true)
    (resultSet.getInt(_: String)).expects("count").returning(count)

    resultSet
  }

  // TODO: Change this based on write modes
  it should "Create a table if it doesn't exist" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.0f)

    val jdbcLayerInterface = mock[JdbcLayerInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(*).returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tableExists _).expects(tablename).returning(Right(false))
    (tableUtils.viewExists _).expects(tablename).returning(Right(false))
    (tableUtils.createTable _).expects(tablename, None, schema, strlen).returning(Right())
    (tableUtils.tableExists _).expects(tablename).returning(Right(true))
    (tableUtils.createAndInitJobStatusTable _).expects(tablename, jdbcConfig.username, "id").returning(Right(true))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.doPreWriteSteps() match {
      case Left(err) => fail(err.msg)
      case Right(_) => ()
    }
  }

  it should "Create a table using custom logic if it doesn't exist" in {
    val createTableStatement = "CREATE table dummy(col1 INTEGER);"

    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = Some(createTableStatement), copyColumnList = None, sessionId = "id", 0.0f)

    val jdbcLayerInterface = mock[JdbcLayerInterface]

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(*).returning(Right(()))

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tableExists _).expects(tablename).returning(Right(false))
    (tableUtils.viewExists _).expects(tablename).returning(Right(false))
    (tableUtils.createTable _).expects(tablename, Some(createTableStatement), schema, strlen).returning(Right())
    (tableUtils.tableExists _).expects(tablename).returning(Right(true))
    (tableUtils.createAndInitJobStatusTable _).expects(tablename, jdbcConfig.username, "id").returning(Right(true))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.doPreWriteSteps() match {
      case Left(err) => fail(err.msg)
      case Right(_) => ()
    }
  }

  it should "Pass on errors from table tools" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.0f)

    val resultSet = mock[ResultSet]
    val jdbcLayerInterface = mock[JdbcLayerInterface]
    val schemaToolsInterface = mock[SchemaToolsInterface]
    val fileStoreLayerInterface = mock[FileStoreLayerInterface]

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tableExists _).expects(tablename).returning(Left(ConnectorError(SchemaConversionError)))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.doPreWriteSteps() match {
      case Left(err) => assert(err.err == SchemaConversionError)
      case Right(_) => fail
    }
  }

  it should "Create the directory for exporting files" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.0f)

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    val schemaToolsInterface = mock[SchemaToolsInterface]

    // Directory w/ configured address is created
    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(fileStoreConfig.address).returning(Right(()))

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tableExists _).expects(*).returning(Right(false))
    (tableUtils.viewExists _).expects(*).returning(Right(false))
    (tableUtils.createTable _).expects(*, *, *, *).returning(Right())
    (tableUtils.tableExists _).expects(*).returning(Right(true))
    (tableUtils.createAndInitJobStatusTable _).expects(*, *, *).returning(Right(true))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.doPreWriteSteps() match {
      case Left(err) => fail(err.msg)
      case Right(_) => ()
    }
  }

  it should "use filestore layer to write data" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.0f)

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

    val tableUtils = mock[TableUtilsInterface]

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.startPartitionWrite(uniqueId))
    checkResult(pipe.writeData(dataBlock))
    checkResult(pipe.endPartitionWrite())
  }

  it should "pass on errors from filestore layer on write" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.0f)

    val uniqueId = "unique-id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.openWriteParquetFile _).expects(*).returning(Left(ConnectorError(OpenWriteError)))

    val tableUtils = mock[TableUtilsInterface]

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.startPartitionWrite(uniqueId) match {
      case Right(_) => fail
      case Left(err) => assert(err.err == OpenWriteError)
    }
  }

  it should "error if view exists with the table name" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.0f)

    val jdbcLayerInterface = mock[JdbcLayerInterface]

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tableExists _).expects(*).returning(Right(false))
    (tableUtils.viewExists _).expects(*).returning(Right(true))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.doPreWriteSteps() match {
      case Left(err) => assert(err.err == ViewExistsError)
      case Right(_) => fail
    }
  }

  it should "call vertica copy upon commit" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.0f)

    val uniqueId = "unique-id"

    val expected = "COPY dummy  FROM 'hdfs://example-hdfs:8020/tmp/test/*.parquet' ON ANY NODE parquet REJECTED DATA AS TABLE dummy_id_COMMITS NO COMMIT"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects("EXPLAIN " + expected).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(expected).returning(Right(1))
    (jdbcLayerInterface.query _).expects("SELECT COUNT(*) as count FROM dummy_id_COMMITS").returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.close _).expects().returning()
    (jdbcLayerInterface.execute _).expects(*).returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename.getFullTableName, schema).returning(Right(""))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(tablename, jdbcConfig.username, 0.0, "id", true).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "call vertica copy upon commit with a custom copy list" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = Some("col1 INTEGER, col2 FLOAT"), sessionId = "id", 0.0f)

    val uniqueId = "unique-id"


    val expected = "COPY dummy (col1 INTEGER, col2 FLOAT) FROM 'hdfs://example-hdfs:8020/tmp/test/*.parquet' ON ANY NODE parquet REJECTED DATA AS TABLE dummy_id_COMMITS NO COMMIT"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects("EXPLAIN " + expected).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(expected).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.close _).expects().returning()
    (jdbcLayerInterface.execute _).expects(*).returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))
    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "pass error upon commit" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.0f)

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Left(JDBCLayerError(SyntaxError)))
    (jdbcLayerInterface.rollback _).expects().returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning()

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename.getFullTableName, schema).returning(Right(""))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.commit() match {
      case Right(_) => fail
      case Left(err) => assert(err.err == CommitError)
    }
  }

  it should "cleanup file dir after commit" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.0f)

    val uniqueId = "unique-id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.execute _).expects(*).returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning()
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename.getFullTableName, schema).returning(Right(""))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(fileStoreConfig.address).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "use specified custom copy columns if specified" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None,
      copyColumnList = Some("col1,col3,col2"), sessionId = "id", 0.0f)

    val uniqueId = "unique-id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects("COPY dummy (col1,col3,col2) FROM 'hdfs://example-hdfs:8020/tmp/test/*.parquet' ON ANY NODE parquet REJECTED DATA AS TABLE dummy_id_COMMITS NO COMMIT").returning(Right(1))
    (jdbcLayerInterface.query _).expects(*).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.execute _).expects(*).returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning()
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "use constructed column list from schema tools on commit" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.0f)

    val uniqueId = "unique-id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects("COPY dummy (\"col1\",\"col5\") FROM 'hdfs://example-hdfs:8020/tmp/test/*.parquet' ON ANY NODE parquet REJECTED DATA AS TABLE dummy_id_COMMITS NO COMMIT").returning(Right(1))
    (jdbcLayerInterface.query _).expects(*).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.execute _).expects(*).returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning()
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename.getFullTableName, schema).returning(Right("(\"col1\",\"col5\")"))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "Fail if rejected row count is above threshold" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.05f)

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(100))
    (jdbcLayerInterface.query _).expects(*).returning(Right(getCountTableResultSet(6)))
    (jdbcLayerInterface.close _).expects().returning()
    (jdbcLayerInterface.rollback _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename.getFullTableName, schema).returning(Right(""))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.commit() match {
      case Right(_) => fail
      case Left(err) => assert(err.err == FaultToleranceTestFail)
    }
  }

  it should "Succeed if rejected row count is below threshold" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.05f)

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(100))
    (jdbcLayerInterface.query _).expects(*).returning(Right(getCountTableResultSet(4)))
    (jdbcLayerInterface.close _).expects().returning()
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename.getFullTableName, schema).returning(Right(""))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "Drop rejects table if there are no rejects" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None, copyColumnList = None, sessionId = "id", 0.05f)

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*).returning(Right(100))
    (jdbcLayerInterface.query _).expects(*).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.close _).expects().returning()
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    // Drop statement
    (jdbcLayerInterface.execute _).expects("DROP TABLE IF EXISTS dummy_id_COMMITS CASCADE").returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename.getFullTableName, schema).returning(Right(""))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }
}
