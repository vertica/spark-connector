package com.vertica.spark.util.table

import java.sql.ResultSet

import ch.qos.logback.classic.Level
import com.vertica.spark.config.{LogProvider, TableName}
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ConnectorErrorType.TableCheckError
import com.vertica.spark.util.error.JDBCLayerError
import com.vertica.spark.util.error.JdbcErrorType.ConnectionError
import com.vertica.spark.util.schema.SchemaToolsInterface
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class TableUtilsTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {

  private val logProvider = LogProvider(Level.ERROR)
  private val strlen = 1024

  private def getTableResultSet(exists: Boolean = false) : ResultSet = {
    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true)
    (resultSet.getInt(_: Int)).expects(1).returning(if(exists) 1 else 0)
    (resultSet.close _).expects().returning()

    resultSet
  }

  private def getCountTableResultSet(count: Int = 0) : ResultSet = {
    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true)
    (resultSet.getInt(_: String)).expects("count").returning(count)

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
}
