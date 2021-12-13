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

import com.vertica.spark.common.TestObjects
import com.vertica.spark.config.{BasicJdbcAuth, DistributedFilesystemWriteConfig, FileStoreConfig, JDBCConfig, JDBCTLSConfig, TableName, ValidColumnList, ValidFilePermissions, AWSOptions}
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.{CommitError, ConnectionDownError, ConnectorError, FaultToleranceTestFail, JdbcSchemaError, OpenWriteError, SchemaConversionError, SyntaxError, ViewExistsError}
import com.vertica.spark.util.schema.SchemaToolsInterface
import com.vertica.spark.util.table.TableUtilsInterface
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class VerticaDistributedFilesystemWritePipeTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {
  private val tablename = TableName("dummy", None)
  private val tempTableName = TableName("dummy_id", None)
  private val jdbcConfig = JDBCConfig(
    "1.1.1.1", 1234, "test", BasicJdbcAuth("test", "test"), JDBCTLSConfig(tlsMode = Require, None, None, None, None))
  private val fileStoreConfig = TestObjects.fileStoreConfig
  private val strlen = 1024

  val schema = new StructType(Array(StructField("col1", IntegerType)))

  private def createWriteConfig() = {
    DistributedFilesystemWriteConfig(
      jdbcConfig = jdbcConfig,
      fileStoreConfig = fileStoreConfig,
      tablename = tablename,
      schema = schema,
      strlen = strlen,
      targetTableSql = None,
      copyColumnList = None,
      sessionId = "id",
      0.0f,
      ValidFilePermissions("777").getOrElse(throw new Exception("File perm error")),
      None
    )
  }

  private def checkResult(result: ConnectorResult[Unit]): Unit = {
    result match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
  }

  private def getCountTableResultSet(count: Int = 0): ResultSet = {
    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true)
    (resultSet.getInt(_: String)).expects("count").returning(count)
    (resultSet.close _).expects().returning(())

    resultSet
  }

  private def getStringResultSet(): ResultSet = {
    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true)
    (resultSet.getString(_: String)).expects("INFER_EXTERNAL_TABLE_DDL").returning("create external table \"testtable\"(\"col1\" int) as copy from \'hdfs://example-hdfs:8020/tmp/testtable.parquet/*.parquet\' parquet")
    (resultSet.close _).expects().returning(())

    resultSet
  }

  private def getEmptyResultSet: ResultSet = {
    val resultSet = mock[ResultSet]
    (resultSet.close _).expects().returning()

    resultSet
  }

  private def tryDoPreWriteSteps(pipe: VerticaDistributedFilesystemWritePipe): Unit = {
    pipe.doPreWriteSteps() match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
  }

  it should "Create a table if it doesn't exist" in {
    val config = createWriteConfig()

    val jdbcLayerInterface = mock[JdbcLayerInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(*,*).returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tableExists _).expects(tablename).returning(Right(false))
    (tableUtils.viewExists _).expects(tablename).returning(Right(false))
    (tableUtils.tempTableExists _).expects(tablename).returning(Right(false))
    (tableUtils.createTable _).expects(tablename, None, schema, strlen).returning(Right())
    (tableUtils.tableExists _).expects(tablename).returning(Right(true))
    (tableUtils.createAndInitJobStatusTable _).expects(tablename, jdbcConfig.auth.user, "id", "APPEND").returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    tryDoPreWriteSteps(pipe)
  }

  it should "Drop the table first if in overwrite mode" in {
    val config = createWriteConfig()
    config.setOverwrite(true)

    val jdbcLayerInterface = mock[JdbcLayerInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(*,*).returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.dropTable _).expects(tablename).returning(Right(()))
    (tableUtils.tableExists _).expects(*).returning(Right(false))
    (tableUtils.viewExists _).expects(*).returning(Right(false))
    (tableUtils.tempTableExists _).expects(tablename).returning(Right(false))
    (tableUtils.createTable _).expects(*, *, *, *).returning(Right())
    (tableUtils.tableExists _).expects(*).returning(Right(true))
    (tableUtils.createAndInitJobStatusTable _).expects(*,*,*,*).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    tryDoPreWriteSteps(pipe)
  }

  it should "Create a table using custom logic if it doesn't exist" in {
    val createTableStatement = "CREATE table dummy(col1 INTEGER);"

    val config = createWriteConfig().copy(targetTableSql = Some(createTableStatement))

    val jdbcLayerInterface = mock[JdbcLayerInterface]

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(*,*).returning(Right(()))

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tableExists _).expects(tablename).returning(Right(false))
    (tableUtils.viewExists _).expects(tablename).returning(Right(false))
    (tableUtils.tempTableExists _).expects(tablename).returning(Right(false))
    (tableUtils.createTable _).expects(tablename, Some(createTableStatement), schema, strlen).returning(Right())
    (tableUtils.tableExists _).expects(tablename).returning(Right(true))
    (tableUtils.createAndInitJobStatusTable _).expects(tablename, jdbcConfig.auth.user, "id", *).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    tryDoPreWriteSteps(pipe)
  }

  it should "Pass on errors from table tools" in {
    val config = createWriteConfig()

    val jdbcLayerInterface = mock[JdbcLayerInterface]

    val schemaToolsInterface = mock[SchemaToolsInterface]
    val fileStoreLayerInterface = mock[FileStoreLayerInterface]

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tableExists _).expects(tablename).returning(Left(SchemaConversionError(mock[ConnectorError])))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.doPreWriteSteps() match {
      case Left(err) => assert(err.getUnderlyingError match {
        case SchemaConversionError(_) => true
        case _ => false
      })
      case Right(_) => fail
    }
  }

  it should "Skip initial creation of table when external table is specified" in {
    val config = createWriteConfig().copy(createExternalTable = Some(NewData))

    val jdbcLayerInterface = mock[JdbcLayerInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(*,*).returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tableExists _).expects(tablename).returning(Right(false))
    (tableUtils.viewExists _).expects(tablename).returning(Right(false))
    (tableUtils.tempTableExists _).expects(tablename).returning(Right(false))
    (tableUtils.tableExists _).expects(tablename).returning(Right(true))
    (tableUtils.createAndInitJobStatusTable _).expects(tablename, jdbcConfig.auth.user, "id", "APPEND").returning(Right(()))

    // Should skip
    (tableUtils.createTable _).expects(tablename, None, schema, strlen).returning(Right()).never()

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    tryDoPreWriteSteps(pipe)
  }

  it should "Create the directory for exporting files" in {
    val config = createWriteConfig()

    val jdbcLayerInterface = mock[JdbcLayerInterface]

    val schemaToolsInterface = mock[SchemaToolsInterface]

    // Directory w/ configured address is created
    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(fileStoreConfig.address, *).returning(Right(()))

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tableExists _).expects(*).returning(Right(false))
    (tableUtils.viewExists _).expects(*).returning(Right(false))
    (tableUtils.tempTableExists _).expects(tablename).returning(Right(false))
    (tableUtils.createTable _).expects(*, *, *, *).returning(Right())
    (tableUtils.tableExists _).expects(*).returning(Right(true))
    (tableUtils.createAndInitJobStatusTable _).expects(*, *, *, *).returning(Right())

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    tryDoPreWriteSteps(pipe)
  }

  it should "error if view exists with the table name" in {
    val config = createWriteConfig()

    val jdbcLayerInterface = mock[JdbcLayerInterface]

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tableExists _).expects(*).returning(Right(false))
    (tableUtils.viewExists _).expects(*).returning(Right(true))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.doPreWriteSteps() match {
      case Left(err) => assert(err.getUnderlyingError == ViewExistsError())
      case Right(_) => fail
    }
  }

  it should "use filestore layer to write data" in {
    val config = createWriteConfig()

    val uniqueId = "unique-id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val v1: Int = 1
    val v2: Float = 2.0f
    val v3: Float = 3.0f
    val dataBlock = DataBlock(List(InternalRow(v1, v2), InternalRow(v1, v3)))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.openWriteParquetFile _).expects(fileStoreConfig.address + "/" + uniqueId + ".snappy.parquet").returning(Right(()))
    (fileStoreLayerInterface.writeDataToParquetFile _).expects(dataBlock).returning(Right(()))
    (fileStoreLayerInterface.closeWriteParquetFile _).expects().returning(Right(()))

    val tableUtils = mock[TableUtilsInterface]

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.startPartitionWrite(uniqueId))
    checkResult(pipe.writeData(dataBlock))
    checkResult(pipe.endPartitionWrite())
  }

  it should "pass on errors from filestore layer on write" in {
    val config = createWriteConfig()

    val uniqueId = "unique-id"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.openWriteParquetFile _).expects(*).returning(Left(OpenWriteError(new Exception())))
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right(()))

    val tableUtils = mock[TableUtilsInterface]

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.startPartitionWrite(uniqueId) match {
      case Right(_) => fail
      case Left(err) => assert(err.getUnderlyingError match {
        case OpenWriteError(_) => true
        case _ => false
      })
    }
  }

  it should "call vertica copy upon commit" in {
    val config = createWriteConfig()

    val uniqueId = "unique-id"

    val expected = "COPY \"dummy\"  FROM 'hdfs://example-hdfs:8020/tmp/test/*.parquet' ON ANY NODE parquet REJECTED DATA AS TABLE \"dummy_id_REJECTS_TMP\" NO COMMIT"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*, *).returning(Right(1))
    (jdbcLayerInterface.query _).expects("EXPLAIN " + expected, *).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.executeUpdate _).expects(expected, *).returning(Right(1))
    (jdbcLayerInterface.query _).expects("SELECT COUNT(*) as count FROM \"dummy_id_REJECTS_TMP\"", *).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename, schema).returning(Right(""))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(tablename, jdbcConfig.auth.user, 0.0, "id", true).returning(Right(()))
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(false))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "call vertica copy upon commit with a custom copy list" in {
    val config = createWriteConfig().copy(copyColumnList = ValidColumnList("col1 INTEGER, col2 FLOAT").getOrElse(None))

    val expected = "COPY \"dummy\" (col1 INTEGER,col2 FLOAT) FROM 'hdfs://example-hdfs:8020/tmp/test/*.parquet' ON ANY NODE parquet REJECTED DATA AS TABLE \"dummy_id_REJECTS_TMP\" NO COMMIT"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*, *).returning(Right(1))
    (jdbcLayerInterface.query _).expects("EXPLAIN " + expected, *).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.executeUpdate _).expects(expected, *).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*, *).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(false))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "pass error upon commit" in {
    val config = createWriteConfig()

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*, *).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*, *).returning(Left(SyntaxError(new Exception())))
    (jdbcLayerInterface.rollback _).expects().returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename, schema).returning(Right(""))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(false))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.commit() match {
      case Right(_) => fail
      case Left(err) => assert(err.getUnderlyingError match {
        case CommitError(_) => true
        case _ => false
      })
    }
  }

  it should "cleanup file dir after commit" in {
    val config = createWriteConfig()

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename, schema).returning(Right(""))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(fileStoreConfig.address).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(false))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "use specified custom copy columns if specified" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = createWriteConfig().copy(
      copyColumnList = ValidColumnList("col1,col3,col2").getOrElse(None)
    )

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.executeUpdate _).expects("COPY \"dummy\" (col1,col3,col2) FROM 'hdfs://example-hdfs:8020/tmp/test/*.parquet' ON ANY NODE parquet REJECTED DATA AS TABLE \"dummy_id_REJECTS_TMP\" NO COMMIT", *).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*, *).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(false))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "use constructed column list from schema tools on commit" in {
    val config = createWriteConfig()

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.executeUpdate _).expects("COPY \"dummy\" (\"col1\",\"col5\") FROM 'hdfs://example-hdfs:8020/tmp/test/*.parquet' ON ANY NODE parquet REJECTED DATA AS TABLE \"dummy_id_REJECTS_TMP\" NO COMMIT",*).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename, schema).returning(Right("(\"col1\",\"col5\")"))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(false))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "Fail if rejected row count is above threshold" in {
    val config = createWriteConfig().copy(
      failedRowPercentTolerance =  0.05f
    )

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(100))
    (jdbcLayerInterface.execute _).expects(*, *).returning(Right(()))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getCountTableResultSet(6)))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.rollback _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename, schema).returning(Right(""))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(false))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.commit() match {
      case Right(_) => fail
      case Left(err) => assert(err.getUnderlyingError == FaultToleranceTestFail())
    }
  }

  it should "Succeed if rejected row count is below threshold" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = createWriteConfig().copy(
      failedRowPercentTolerance =  0.05f
    )

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(100))
    (jdbcLayerInterface.execute _).expects(*, *).returning(Right(()))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getCountTableResultSet(4)))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename, schema).returning(Right(""))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(false))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "create an external table" in {
    val tname = TableName("testtable", None)
    val config = createWriteConfig().copy(createExternalTable = Some(NewData), tablename = tname)

    val expectedUrl = config.fileStoreConfig.externalTableAddress + "*.parquet"

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.commit _).expects().returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.configureSession _).expects(fileStoreLayerInterface).returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.createExternalTable _).expects(tname, config.targetTableSql, config.schema, config.strlen, expectedUrl).returning(Right(()))
    (tableUtils.validateExternalTable _).expects(tname).returning(Right(()))
    (tableUtils.updateJobStatusTable _).expects(tname, jdbcConfig.auth.user, 0.0, "id", true).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "create an external table with existing data" in {
    val tname = TableName("testtable", None)
    val config = createWriteConfig().copy(createExternalTable = Some(ExistingData),
      fileStoreConfig = fileStoreConfig.copy("hdfs://example-hdfs:8020/tmp/testtable.parquet"),
      tablename = TableName("testtable", None), schema = new StructType())
    val url = "hdfs://example-hdfs:8020/tmp/testtable.parquet/*.parquet"

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.getGlobStatus _).expects(url).returning(Right(Array[String]("example.parquet")))
    val inferStmt = s"SELECT INFER_EXTERNAL_TABLE_DDL(\'$url\',\'testtable\')"
    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(inferStmt,*).returning(Right(getStringResultSet))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.configureSession _).expects(fileStoreLayerInterface).returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    val createExternalTableStmt = "create external table testtable(\"col1\" int) " +
      "as copy from \'hdfs://example-hdfs:8020/tmp/testtable.parquet/*.parquet\' parquet"

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.createExternalTable _).expects(tname, Some(createExternalTableStmt), config.schema, config.strlen, url).returning(Right(()))
    (tableUtils.validateExternalTable _).expects(tname).returning(Right(()))
    (tableUtils.updateJobStatusTable _).expects(tname, jdbcConfig.auth.user, 0.0, "id", true).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }


  it should "handle failure creating external table" in {
    val tname = TableName("testtable", None)
    val config = createWriteConfig().copy(createExternalTable = Some(NewData), tablename = tname)

    val expectedUrl = config.fileStoreConfig.externalTableAddress + "*.parquet"

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.rollback _).expects().returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.configureSession _).expects(fileStoreLayerInterface).returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.createExternalTable _).expects(tname, config.targetTableSql, config.schema, config.strlen, expectedUrl).returning(Left(ConnectionDownError()))
    (tableUtils.dropTable _).expects(tname).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.commit() match {
      case Right(()) => fail
      case Left(err) => assert(err.isInstanceOf[ConnectionDownError])
    }
  }

  it should "handle failure validating external table" in {
    val tname = TableName("testtable", None)
    val config = createWriteConfig().copy(createExternalTable = Some(NewData), tablename = tname)

    val expectedUrl = config.fileStoreConfig.externalTableAddress + "*.parquet"

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.rollback _).expects().returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.configureSession _).expects(fileStoreLayerInterface).returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.createExternalTable _).expects(tname, config.targetTableSql, config.schema, config.strlen, expectedUrl).returning(Right())
    (tableUtils.validateExternalTable _).expects(tname).returning(Left(ConnectionDownError()))
    (tableUtils.dropTable _).expects(tname).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.commit() match {
      case Right(()) => fail
      case Left(err) => assert(err.isInstanceOf[ConnectionDownError])
    }
  }

  it should "Create temp table for merge" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = createWriteConfig().copy(
      mergeKey =  ValidColumnList("col1").getOrElse(None)
    )
    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.execute _).expects(*,*).returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename, schema).returning(Right("(col1)"))
    (schemaToolsInterface.getMergeUpdateValues _).expects(jdbcLayerInterface, tablename, tempTableName, config.copyColumnList).returning(Right("col1=temp.col1"))
    (schemaToolsInterface.getMergeInsertValues _).expects(jdbcLayerInterface, tempTableName, config.copyColumnList).returning(Right("temp.col1"))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))
    (tableUtils.createTempTable _).expects(tempTableName, schema, strlen).returning(Right(()))
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(true))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)
    checkResult(pipe.commit())
  }

  it should "Copy data into temporary table if mergeKey exists" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = createWriteConfig().copy(
      mergeKey =  ValidColumnList("col1").getOrElse(None)
    )
    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects("COPY \"dummy_id\" FROM 'hdfs://example-hdfs:8020/tmp/test/*.parquet' ON ANY NODE parquet REJECTED DATA AS TABLE \"dummy_id_REJECTS_TMP\" NO COMMIT",*).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.execute _).expects(*,*).returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename, schema).returning(Right("(col1)"))
    (schemaToolsInterface.getMergeUpdateValues _).expects(jdbcLayerInterface, tablename, tempTableName, config.copyColumnList).returning(Right("col1=temp.col1"))
    (schemaToolsInterface.getMergeInsertValues _).expects(jdbcLayerInterface, tempTableName, config.copyColumnList).returning(Right("temp.col1"))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))
    (tableUtils.createTempTable _).expects(tempTableName, schema, strlen).returning(Right(()))
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(true))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)
    checkResult(pipe.commit())
  }

  it should "Use custom copy list in merge" in {
    val config = createWriteConfig().copy(
      mergeKey =  ValidColumnList("col1").getOrElse(None),
      copyColumnList = ValidColumnList("a,b").getOrElse(None),
      schema = new StructType(Array(StructField("col1", IntegerType), StructField("col2", IntegerType)))
    )
    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(1))
    (jdbcLayerInterface.executeUpdate _).expects(*,*).returning(Right(1))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.query _).expects(*,*).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.execute _).expects(*,*).returning(Right(()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getMergeUpdateValues _).expects(jdbcLayerInterface, tablename, tempTableName, config.copyColumnList).returning(Right("col1=temp.col1"))
    (schemaToolsInterface.getMergeInsertValues _).expects(jdbcLayerInterface, tempTableName, config.copyColumnList).returning(Right("temp.col1"))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.removeDir _).expects(*).returning(Right())

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))
    (tableUtils.createTempTable _).expects(tempTableName, config.schema, strlen).returning(Right(()))
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(true))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)
    checkResult(pipe.commit())
  }

  it should "prevent cleanup of parquet files when prevent_cleanup set to true" in {
    val fsConfig: FileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/", "test", true, AWSOptions(None, None, None, None, None, None))
    val config = createWriteConfig().copy(fileStoreConfig = fsConfig)

    val expected = "COPY \"dummy\"  FROM 'hdfs://example-hdfs:8020/tmp/test/*.parquet' ON ANY NODE parquet REJECTED DATA AS TABLE \"dummy_id_REJECTS_TMP\" NO COMMIT"

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.configureSession _).expects(*).returning(Right(()))
    (jdbcLayerInterface.executeUpdate _).expects(*, *).returning(Right(1))
    (jdbcLayerInterface.query _).expects("EXPLAIN " + expected, *).returning(Right(getEmptyResultSet))
    (jdbcLayerInterface.executeUpdate _).expects(expected, *).returning(Right(1))
    (jdbcLayerInterface.query _).expects("SELECT COUNT(*) as count FROM \"dummy_id_REJECTS_TMP\"", *).returning(Right(getCountTableResultSet()))
    (jdbcLayerInterface.close _).expects().returning(Right(()))
    (jdbcLayerInterface.commit _).expects().returning(Right(()))

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getCopyColumnList _).expects(jdbcLayerInterface, tablename, schema).returning(Right(""))

    val tableUtils = mock[TableUtilsInterface]
    (tableUtils.updateJobStatusTable _).expects(*, *, *, *, *).returning(Right(()))
    (tableUtils.tempTableExists _).expects(tempTableName).returning(Right(false))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    checkResult(pipe.commit())
  }

  it should "prevent cleanup if startPartitionWrite returns an error" in {
    val fsConfig: FileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/", "test", true, AWSOptions(None, None, None, None, None, None))
    val config = createWriteConfig().copy(fileStoreConfig = fsConfig)
    val uniqueId = "unique-id"
    val jdbcLayerInterface = mock[JdbcLayerInterface]
    val schemaToolsInterface = mock[SchemaToolsInterface]

    val v1: Int = 1
    val v2: Float = 2.0f
    val v3: Float = 3.0f
    val dataBlock = DataBlock(List(InternalRow(v1, v2), InternalRow(v1, v3)))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.openWriteParquetFile _).expects(fileStoreConfig.address + "/" + uniqueId + ".snappy.parquet").returning((Left(OpenWriteError(new Exception()))))

    val tableUtils = mock[TableUtilsInterface]

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface, tableUtils)

    pipe.startPartitionWrite(uniqueId) match {
      case Right(_) => fail
      case Left(err) => assert(err.getUnderlyingError match {
        case OpenWriteError(_) => true
        case _ => false
      })
    }
  }
}
