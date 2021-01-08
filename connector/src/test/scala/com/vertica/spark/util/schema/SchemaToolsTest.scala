import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import org.scalamock.scalatest.MockFactory
import java.sql.ResultSet
import java.sql.ResultSetMetaData

import com.vertica.spark.util.schema._
import com.vertica.spark.jdbc._

import org.apache.spark.sql.types._

import com.vertica.spark.util.error._
import com.vertica.spark.util.error.SchemaErrorType._
import com.vertica.spark.util.error.JdbcErrorType._

case class ColumnDef(index: Int, name: String, colType: Integer, colTypeName: String, scale: Integer, signed: Boolean, nullable: Boolean){
}

/**
  * Tests functionality of schema tools: converting schema between JDBC and Spark types.
  *
  * Tests here not exhaustive of all possible options, covers common cases and edge cases. Integration tests should cover a wider selection of closer to real world cases of table schema.
  */
class SchemaToolsTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {

  private def mockJdbcDeps(tablename: String): (JdbcLayerInterface, ResultSet, ResultSetMetaData) = {
    val jdbcLayer = mock[JdbcLayerInterface]
    val resultSet = mock[ResultSet]
    val rsmd = mock[ResultSetMetaData]

    (jdbcLayer.query _).expects("SELECT * FROM " + tablename + " WHERE 1=0").returning(Right(resultSet))
    (resultSet.getMetaData _).expects().returning(rsmd)
    (resultSet.close _).expects()

    (jdbcLayer, resultSet, rsmd)

  }

  private def mockColumnMetadata(rsmd: ResultSetMetaData, col: ColumnDef) = {
    (rsmd.getColumnLabel _).expects(col.index).returning(col.name)
    (rsmd.getColumnType _).expects(col.index).returning(col.colType)
    (rsmd.getColumnTypeName _).expects(col.index).returning(col.colTypeName)
    (rsmd.getScale _).expects(col.index).returning(col.scale)
    (rsmd.isSigned _).expects(col.index).returning(col.signed)
    (rsmd.isNullable _).expects(col.index).returning(if(col.nullable) ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls)
  }

  private def mockColumnCount(rsmd: ResultSetMetaData, count: Integer) = {
    (rsmd.getColumnCount _).expects().returning(count)
  }

  val tablename = "testtable"

  it should "parse a single-column double schema" in {
    SchemaTools.impl = new SchemaToolsDefaultImpl

    val (jdbcLayer, resultSet, rsmd) = mockJdbcDeps(tablename)

    // Schema
    mockColumnMetadata(rsmd, new ColumnDef(1, "col1", java.sql.Types.REAL, "REAL", 32, false, true))
    mockColumnCount(rsmd, 1)

    SchemaTools.readSchema(jdbcLayer, tablename) match {
      case Left(err) => assert(false)
      case Right(schema) => {
        val field = schema.fields(0)
        assert(field.name == "col1")
        assert(field.nullable == true)
        assert(field.dataType == DoubleType)
      }
    }
  }

  it should "parse a multi-column schema" in {
    val (jdbcLayer, resultSet, rsmd) = mockJdbcDeps(tablename)

    // Schema
    mockColumnMetadata(rsmd, new ColumnDef(1, "col1", java.sql.Types.REAL, "REAL", 32, false, true))
    mockColumnMetadata(rsmd, new ColumnDef(2, "col2", java.sql.Types.VARCHAR, "VARCHAR", 0, false, true))
    mockColumnMetadata(rsmd, new ColumnDef(3, "col3", java.sql.Types.BIGINT, "BIGINT", 0, true, true))
    mockColumnCount(rsmd, 3)

    SchemaTools.readSchema(jdbcLayer, tablename) match {
      case Left(err) => assert(false)
      case Right(schema) => {
        val fields = schema.fields
        assert(fields(0).name == "col1")
        assert(fields(0).nullable == true)
        assert(fields(0).dataType == DoubleType)

        assert(fields(1).name == "col2")
        assert(fields(1).nullable == true)
        assert(fields(1).dataType == StringType)

        assert(fields(2).name == "col3")
        assert(fields(2).nullable == true)
        assert(fields(2).dataType == LongType)
      }
    }
  }

  it should "parse BIGINT" in {
    val (jdbcLayer, resultSet, rsmd) = mockJdbcDeps(tablename)

    // Signed BIGINT represented by long type
    mockColumnMetadata(rsmd, new ColumnDef(1, "col1", java.sql.Types.BIGINT, "BIGINT", 32, true, true))
    // Unsigned BIGINT represented by decimal type without any digits after the 0
    mockColumnMetadata(rsmd, new ColumnDef(2, "col2", java.sql.Types.BIGINT, "BIGINT", 0, false, true))
    mockColumnCount(rsmd, 2)

    SchemaTools.readSchema(jdbcLayer, tablename) match {
      case Left(err) => assert(false)
      case Right(schema) => {
        val fields = schema.fields
        assert(fields(0).name == "col1")
        assert(fields(0).dataType == LongType)

        assert(fields(1).name == "col2")
        assert(fields(1).dataType == DecimalType(DecimalType.MAX_PRECISION,0))
      }
    }
  }

  it should "parse DECIMAL | NUMERIC" in {
    val (jdbcLayer, resultSet, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, new ColumnDef(1, "col1", java.sql.Types.NUMERIC, "NUMERIC", 32, true, true))
    mockColumnMetadata(rsmd, new ColumnDef(2, "col2", java.sql.Types.DECIMAL, "VARCHAR", 16, false, true))
    mockColumnCount(rsmd, 2)

    SchemaTools.readSchema(jdbcLayer, tablename) match {
      case Left(err) => assert(false)
      case Right(schema) => {
        val fields = schema.fields
        assert(fields(0).name == "col1")
        assert(fields(0).dataType == DecimalType(DecimalType.MAX_PRECISION, 32))

        assert(fields(1).name == "col2")
        assert(fields(1).dataType == DecimalType(DecimalType.MAX_PRECISION, 16))
      }
    }
  }

  it should "fail when trying to parse invalid types" in {
    val (jdbcLayer, resultSet, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, new ColumnDef(1, "col1", 50000, "invalid-type", 16, false, true))
    mockColumnMetadata(rsmd, new ColumnDef(2, "col2", 50000, "invalid-type", 16, false, true))
    mockColumnCount(rsmd, 2)

    SchemaTools.readSchema(jdbcLayer, tablename) match {
      case Left(errList) => {
        assert(errList.size == 2)
        assert(errList(0).err == MissingConversionError)
        assert(errList(1).err == MissingConversionError)
      }
      case Right(schema) => assert(false)
    }
  }

  it should "fail when there's an error connecting to database" in {
    val jdbcLayer = mock[JdbcLayerInterface]

    (jdbcLayer.query _).expects("SELECT * FROM " + tablename + " WHERE 1=0").returning(Left(JDBCLayerError(ConnectionError)))

    SchemaTools.readSchema(jdbcLayer, tablename) match {
      case Left(errList) => {
        assert(errList.size == 1)
        assert(errList(0).err == JdbcError)
      }
      case Right(schema) => assert(false)
    }
  }
}
