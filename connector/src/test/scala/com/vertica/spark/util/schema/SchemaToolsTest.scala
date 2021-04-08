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

package com.vertica.spark.util.schema

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalamock.scalatest.MockFactory
import java.sql.ResultSet
import java.sql.ResultSetMetaData

import ch.qos.logback.classic.Level
import com.vertica.spark.config.LogProvider
import com.vertica.spark.datasource.jdbc._
import org.apache.spark.sql.types._
import com.vertica.spark.util.error._

case class TestColumnDef(index: Int, name: String, colType: Int, colTypeName: String, scale: Int, signed: Boolean, nullable: Boolean)

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

    (jdbcLayer.query _).expects("SELECT * FROM " + tablename + " WHERE 1=0", *).returning(Right(resultSet))
    (resultSet.getMetaData _).expects().returning(rsmd)
    (resultSet.close _).expects()

    (jdbcLayer, resultSet, rsmd)

  }

  private def mockColumnMetadata(rsmd: ResultSetMetaData, col: TestColumnDef) = {
    (rsmd.getColumnLabel _).expects(col.index).returning(col.name)
    (rsmd.getColumnType _).expects(col.index).returning(col.colType)
    (rsmd.getColumnTypeName _).expects(col.index).returning(col.colTypeName)
    (rsmd.getScale _).expects(col.index).returning(col.scale)
    (rsmd.isSigned _).expects(col.index).returning(col.signed)
    (rsmd.isNullable _).expects(col.index).returning(if(col.nullable) ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls)
  }

  private def mockColumnCount(rsmd: ResultSetMetaData, count: Int) = {
    (rsmd.getColumnCount _).expects().returning(count)
  }

  val tablename = "\"testtable\""

  it should "parse a single-column double schema" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    // Schema
    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.REAL, "REAL", 32, signed = false, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(err) => fail(err.toString())
      case Right(schema) =>
        val field = schema.fields(0)
        assert(field.name == "col1")
        assert(field.nullable)
        assert(field.dataType == DoubleType)
    }
  }

  it should "parse a multi-column schema" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    // Schema
    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.REAL, "REAL", 32, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col2", java.sql.Types.VARCHAR, "VARCHAR", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(3, "col3", java.sql.Types.BIGINT, "BIGINT", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 3)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields(0).name == "col1")
        assert(fields(0).nullable)
        assert(fields(0).dataType == DoubleType)

        assert(fields(1).name == "col2")
        assert(fields(1).nullable)
        assert(fields(1).dataType == StringType)

        assert(fields(2).name == "col3")
        assert(fields(2).nullable)
        assert(fields(2).dataType == LongType)
    }
  }

  it should "parse BIGINT" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    // Signed BIGINT represented by long type
    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.BIGINT, "BIGINT", 32, signed = true, nullable = true))
    // Unsigned BIGINT represented by decimal type without any digits after the 0
    mockColumnMetadata(rsmd, TestColumnDef(2, "col2", java.sql.Types.BIGINT, "BIGINT", 0, signed = false, nullable = true))
    mockColumnCount(rsmd, 2)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields(0).name == "col1")
        assert(fields(0).dataType == LongType)

        assert(fields(1).name == "col2")
        assert(fields(1).dataType == DecimalType(DecimalType.MAX_PRECISION,0))
    }
  }

  it should "parse decimal types" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.NUMERIC, "NUMERIC", 32, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col2", java.sql.Types.DECIMAL, "VARCHAR", 16, signed = false, nullable = true))
    mockColumnCount(rsmd, 2)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields(0).name == "col1")
        assert(fields(0).dataType == DecimalType(DecimalType.MAX_PRECISION, 32))

        assert(fields(1).name == "col2")
        assert(fields(1).dataType == DecimalType(DecimalType.MAX_PRECISION, 16))
    }
  }

  it should "parse int types" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.INTEGER, "INTEGER", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col2", java.sql.Types.SMALLINT, "SMALLINT", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(3, "col3", java.sql.Types.TINYINT, "TINYINT", 0, signed = false, nullable = true))
    mockColumnCount(rsmd, 3)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields(0).dataType == IntegerType)
        assert(fields(1).dataType == IntegerType)
        assert(fields(2).dataType == IntegerType)
    }
  }

  it should "parse binary types" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.BINARY, "BINARY", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col2", java.sql.Types.BLOB, "BLOB", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(3, "col3", java.sql.Types.VARBINARY, "VARBINARY", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(4, "col4", java.sql.Types.LONGVARBINARY, "LONGVARBINARY", 0, signed = false, nullable = true))
    mockColumnCount(rsmd, 4)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields(0).dataType == BinaryType)

        assert(fields(1).dataType == BinaryType)

        assert(fields(2).dataType == BinaryType)

        assert(fields(3).dataType == BinaryType)
    }
  }

  it should "parse boolean types" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.BOOLEAN, "BOOLEAN", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col2", java.sql.Types.BIT, "BIT", 0, signed = false, nullable = true))
    mockColumnCount(rsmd, 2)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields(0).name == "col1")
        assert(fields(0).dataType == BooleanType)

        assert(fields(1).name == "col2")
        assert(fields(1).dataType == BooleanType)
    }
  }

  it should "parse string types" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    // All these types are strings or to be converted as such
    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.CHAR, "CHAR", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col2", java.sql.Types.CLOB, "CLOB", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(3, "col3", java.sql.Types.LONGNVARCHAR, "LONGNVARCHAR", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(4, "col4", java.sql.Types.LONGVARCHAR, "LONGVARCHAR", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(5, "col5", java.sql.Types.NCHAR, "NCHAR", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(6, "col6", java.sql.Types.NCLOB, "NCLOB", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(7, "col7", java.sql.Types.NVARCHAR, "NVARCHAR", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(8, "col8", java.sql.Types.REF, "REF", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(9, "col9", java.sql.Types.SQLXML, "SQLXML", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(10, "col10", java.sql.Types.STRUCT, "STRUCT", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(11, "col11", java.sql.Types.VARCHAR, "VARCHAR", 0, signed = false, nullable = true))
    mockColumnCount(rsmd, 11)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 11)
        for(field <- fields) {
          assert(field.dataType == StringType)
        }
    }
  }

  it should "parse date type" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.DATE, "DATE", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields(0).dataType == DateType)
    }
  }

  it should "parse float type" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.FLOAT, "FLOAT", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields(0).dataType == FloatType)
    }
  }

  it should "parse timestamp type" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.TIMESTAMP, "TIMESTAMP", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields(0).dataType == TimestampType)
    }
  }

  it should "parse time type to string" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.TIME, "TIME", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields(0).dataType == StringType)
    }
  }

  it should "parse interval type to string" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.OTHER, "interval", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields(0).dataType == StringType)
    }
  }

  it should "parse uuid type to string" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.OTHER, "uuid", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields(0).dataType == StringType)
    }
  }

  it should "parse other type with error" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.OTHER, "asdf", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(e) => e.isInstanceOf[MissingSqlConversionError]
      case Right(_) => fail
    }
  }

  it should "parse rowid to long type" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.ROWID, "ROWID", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields(0).dataType == LongType)
    }
  }

  it should "parse double types" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.DOUBLE, "DOUBLE", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col2", java.sql.Types.REAL, "REAL", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 2)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(_) => fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 2)
        assert(fields(0).dataType == DoubleType)
        assert(fields(1).dataType == DoubleType)
    }
  }

  it should "error on unsupported types" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    //mockColumnMetadata(rsmd, ColumnDef(1, "col1", java.sql.Types.ARRAY, "ARRAY", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(1, "col2", java.sql.Types.DATALINK, "DATALINK", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col3", java.sql.Types.DISTINCT, "DISTINCT", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(3, "col4", java.sql.Types.JAVA_OBJECT, "JAVA_OBJECT", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(4, "col5", java.sql.Types.NULL, "NULL", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 4)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(err) => assert(err.getError match {
          case ErrorList(errors) => errors.size == 4
          case _ => false
        })
      case Right(_) => fail
    }
  }

  it should "fail when trying to parse invalid types" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", 50000, "invalid-type", 16, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col2", 50000, "invalid-type", 16, signed = false, nullable = true))
    mockColumnCount(rsmd, 2)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(err) =>
        assert(err.getError match {
          case ErrorList(errors) => errors.size == 2
          case _ => false
        })
        assert(err.getError match {
          case ErrorList(errors) => errors.head match {
            case MissingSqlConversionError(_, _) => true
            case _ => false
          }
          case _ => false
        })
        assert(err.getError match {
          case ErrorList(errors) => errors.tail.head match {
            case MissingSqlConversionError(_, _) => true
            case _ => false
          }
          case _ => false
        })
      case Right(_) => fail
    }
  }

  it should "provide a good error message when trying to convert invalid SQL types to Spark types" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", 50000, "invalid-type", 16, signed = false, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(err) =>
        err.getError match {
          case ErrorList(errors) => errors.toList.foreach(error => assert(error.getUserMessage ==
            "Could not find conversion for unsupported SQL type: invalid-type" +
            "\nSQL type value: 50000"))
          case _ => false
        }
      case Right(_) => fail
    }
  }

  it should "provide a good error message when trying to convert invalid Spark types to SQL types" in {
    (new SchemaTools).getVerticaTypeFromSparkType(CharType(0), 0) match {
      case Left(err) =>
        err.getError match {
          case ErrorList(errors) => errors.toList.foreach(error => assert(error.getUserMessage ==
            "Could not find conversion for unsupported Spark type: CharType"))
          case _ => false
        }
      case Right(_) => fail
    }
  }

  it should "fail when there's an error connecting to database" in {
    val jdbcLayer = mock[JdbcLayerInterface]

    (jdbcLayer.query _).expects(*,*).returning(Left(ConnectionError(new Exception())))

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(err) =>
        assert(err.getError match {
          case JdbcSchemaError(_) => true
          case _ => false
        })
      case Right(_) => fail
    }
  }

  it should "Convert basic spark types to vertica types" in {
    val schemaTools = new SchemaTools

    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.BinaryType, 1) == Right("VARBINARY(65000)"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.BooleanType, 1) == Right("BOOLEAN"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.ByteType, 1 ) == Right("TINYINT"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.DateType, 1 ) == Right("DATE"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.CalendarIntervalType, 1) == Right("INTERVAL"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.DoubleType, 1 ) == Right("DOUBLE PRECISION"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.DecimalType(0,0), 1 ) == Right("DECIMAL"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.FloatType, 1 ) == Right("FLOAT"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.IntegerType, 1 ) == Right("INTEGER"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.LongType, 1 ) == Right("BIGINT"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.NullType, 1 ) == Right("null"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.ShortType, 1 ) == Right("SMALLINT"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.TimestampType, 1 ) == Right("TIMESTAMP"))
  }


  it should "Convert string types to vertica type properly" in {
    val schemaTools = new SchemaTools

    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.StringType, 1024) == Right("VARCHAR(1024)"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.StringType, 5000) == Right("VARCHAR(5000)"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.StringType, 65000) == Right("VARCHAR(65000)"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.StringType, 100000) == Right("LONG VARCHAR(100000)"))
  }

  it should "Return a list of column names to use for copy statement" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    val schema = new StructType(Array(StructField("col1", DateType, nullable = true), StructField("col2", IntegerType, nullable = true)))
    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.DATE, "DATE", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col2", java.sql.Types.INTEGER, "INTEGER", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 2)

    val schemaTools = new SchemaTools

    schemaTools.getCopyColumnList(jdbcLayer, tablename, schema) match {
      case Left(err) => fail(err.getFullContext)
      case Right(str) => assert(str == "(\"col1\",\"col2\")")
    }
  }
}
