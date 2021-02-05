package com.vertica.spark.datasource.core

import java.sql.ResultSet

import ch.qos.logback.classic.Level
import com.vertica.spark.config.{DistributedFilesystemWriteConfig, FileStoreConfig, JDBCConfig, TableName}
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ConnectorErrorType.{SchemaConversionError, TableCheckError}
import com.vertica.spark.util.error.JdbcErrorType.SyntaxError
import com.vertica.spark.util.error.{JDBCLayerError, SchemaError}
import com.vertica.spark.util.error.SchemaErrorType.MissingConversionError
import com.vertica.spark.util.schema.SchemaToolsInterface
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class VerticaDistributedFilesystemWritePipeTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {
  private val tablename = TableName("dummy", None)
  private val jdbcConfig = JDBCConfig("1.1.1.1", 1234, "test", "test", "test", Level.ERROR)
  private val fileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/test", Level.ERROR)
  private val strlen = 1024

  // TODO: Change this based on write modes
  it should "Create a table if it doesn't exist" in {
    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = None)

    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true).twice()
    (resultSet.getBoolean(_: Int)).expects(1).returning(true).twice()

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
    (resultSet.getBoolean(_: Int)).expects(1).returning(true).twice()

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
    (resultSet.getBoolean(_: Int)).expects(1).returning(true)

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
    (resultSet.getBoolean(_: Int)).expects(1).returning(true).twice()

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects(*).returning(Right(resultSet)).twice()
    (jdbcLayerInterface.execute _).expects(*).returning(Right())

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getVerticaTypeFromSparkType _).expects(*, *).returning(Right("INTEGER"))

    // Directory w/ configured address is created
    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(fileStoreConfig.address).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface , mock[SessionIdInterface])

    pipe.doPreWriteSteps() match {
      case Left(err) => fail(err.msg)
      case Right(_) => ()
    }
  }


}
