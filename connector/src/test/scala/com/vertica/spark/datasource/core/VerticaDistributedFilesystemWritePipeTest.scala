package com.vertica.spark.datasource.core

import java.sql.{ResultSet, ResultSetMetaData}

import ch.qos.logback.classic.Level
import com.vertica.spark.config.{DistributedFilesystemReadConfig, DistributedFilesystemWriteConfig, FileStoreConfig, JDBCConfig, TableName}
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
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
    (resultSet.next _).expects().returning(true)
    (resultSet.getBoolean(_: Int)).expects(1).returning(true)

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects("select count(*) from v_catalog.tables where table_schema ILIKE 'public' and table_name ILIKE 'dummy'").returning(Right(resultSet))
    (jdbcLayerInterface.execute _).expects("CREATE table \"dummy\" (\"col1\" INTEGER)  INCLUDE SCHEMA PRIVILEGES ").returning(Right())

    val schemaToolsInterface = mock[SchemaToolsInterface]
    (schemaToolsInterface.getVerticaTypeFromSparkType _).expects(IntegerType, strlen).returning(Right("INTEGER"))

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(*).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface , mock[SessionIdInterface])

    pipe.doPreWriteSteps()
  }

  it should "Create a table using custom logic if it doesn't exist" in {
    val createTableStatement = "CREATE table dummy(col1 INTEGER);"

    val schema = new StructType(Array(StructField("col1", IntegerType)))
    val config = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = schema, strlen = strlen, targetTableSql = Some(createTableStatement))

    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true)
    (resultSet.getBoolean(_: Int)).expects(1).returning(true)

    val jdbcLayerInterface = mock[JdbcLayerInterface]
    (jdbcLayerInterface.query _).expects("select count(*) from v_catalog.tables where table_schema ILIKE 'public' and table_name ILIKE 'dummy'").returning(Right(resultSet))
    (jdbcLayerInterface.execute _).expects(createTableStatement).returning(Right())

    val schemaToolsInterface = mock[SchemaToolsInterface]

    val fileStoreLayerInterface = mock[FileStoreLayerInterface]
    (fileStoreLayerInterface.createDir _).expects(*).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemWritePipe(config, fileStoreLayerInterface, jdbcLayerInterface, schemaToolsInterface , mock[SessionIdInterface])

    pipe.doPreWriteSteps()
  }

  it should "Pass on errors from schema tools" in {
  }

  it should "Pass on errors from JDBC" in {
  }

  it should "Create the directory for exporting files" in {
  }


}
