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

import com.vertica.spark.config.{TableName, TableQuery}
import com.vertica.spark.datasource.jdbc._
import com.vertica.spark.util.error._
import com.vertica.spark.util.query.VerticaTableTests
import com.vertica.spark.util.schema.ComplexTypesSchemaTools.VERTICA_NATIVE_ARRAY_BASE_ID
import org.apache.spark.sql.types._
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.{ResultSet, ResultSetMetaData}

case class TestColumnDef(index: Int, name: String, colType: Int, colTypeName: String, scale: Int, signed: Boolean, nullable: Boolean)

case class TestVerticaTableDef(name: String, schema: Option[String])

case class TestVerticaTypeDef(name: String = "",verticaTypeId: Long, jdbcTypeId: Long, typeName: String, size: Int, scale: Int, children: Seq[TestVerticaTypeDef] = List.empty)

/**
 * Tests functionality of schema tools: converting schema between JDBC and Spark types.
 *
 * Tests here not exhaustive of all possible options, covers common cases and edge cases. Integration tests should cover a wider selection of closer to real world cases of table schema.
 */
class SchemaToolsTests extends AnyFlatSpec with MockFactory with org.scalatest.OneInstancePerTest {

  private[schema] def mockJdbcDeps(tablename: TableName): (JdbcLayerInterface, ResultSet, ResultSetMetaData) = {
    val jdbcLayer = mock[JdbcLayerInterface]
    val resultSet = mock[ResultSet]
    val rsmd = mock[ResultSetMetaData]

    (jdbcLayer.query _).expects("SELECT * FROM " + tablename.getFullTableName + " WHERE 1=0", *).returning(Right(resultSet))
    (resultSet.getMetaData _).expects().returning(rsmd)
    (resultSet.close _).expects()

    (jdbcLayer, resultSet, rsmd)
  }

  private[schema] def mockJdbcDepsQuery(query: TableQuery): (JdbcLayerInterface, ResultSet, ResultSetMetaData) = {
    val jdbcLayer = mock[JdbcLayerInterface]
    val resultSet = mock[ResultSet]
    val rsmd = mock[ResultSetMetaData]

    (jdbcLayer.query _).expects("SELECT * FROM (" + query.query + ") AS x WHERE 1=0", *).returning(Right(resultSet))
    (resultSet.getMetaData _).expects().returning(rsmd)
    (resultSet.close _).expects()

    (jdbcLayer, resultSet, rsmd)

  }

  private[schema] def mockColumnMetadata(rsmd: ResultSetMetaData, col: TestColumnDef) = {
    (rsmd.getColumnLabel _).expects(col.index).returning(col.name)
    (rsmd.getColumnType _).expects(col.index).returning(col.colType)
    (rsmd.getColumnTypeName _).expects(col.index).returning(col.colTypeName)
    (rsmd.getScale _).expects(col.index).returning(col.scale)
    (rsmd.isSigned _).expects(col.index).returning(col.signed)
    (rsmd.isNullable _).expects(col.index).returning(if(col.nullable) ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls)
  }

  private[schema] def mockColumnCount(rsmd: ResultSetMetaData, count: Int): Unit = {
    (rsmd.getColumnCount _).expects().returning(count)
  }

  val tablename = TableName("\"testtable\"", None)

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

  it should "parse schema of query" in {
    val query = TableQuery("SELECT * FROM t WHERE a > 1", "", None)
    val (jdbcLayer, _, rsmd) = mockJdbcDepsQuery(query)

    // Schema
    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.REAL, "REAL", 32, signed = false, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, query) match {
      case Left(err) => fail(err.getFullContext)
      case Right(schema) =>
        val field = schema.fields(0)
        assert(field.name == "col1")
        assert(field.nullable)
        assert(field.dataType == DoubleType)
    }
  }

  it should "fail when query's schema contains complex types" in {
    val query = TableQuery("SELECT * FROM t WHERE a > 1", "", None)
    val (jdbcLayer, _, rsmd) = mockJdbcDepsQuery(query)

    // Schema
    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.REAL, "REAL", 32, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col2", java.sql.Types.ARRAY, "ARRAY", 0, signed = false, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(3, "col3", java.sql.Types.STRUCT, "STRUCT", 0, signed = false, nullable = true))
    mockColumnCount(rsmd, 3)

    (new SchemaTools).readSchema(jdbcLayer, query) match {
      case Left(err) =>
        assert(err.isInstanceOf[ErrorList])
        assert(err.asInstanceOf[ErrorList].errors.length == 2)
        err.asInstanceOf[ErrorList].errors.map(err => assert(err.isInstanceOf[QueryReturnsComplexTypes]))
        succeed
      case Right(schema) => fail("Expected to fail")
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
      case Left(err) => fail(err.getFullContext)
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
    mockColumnMetadata(rsmd, TestColumnDef(10, "col11", java.sql.Types.VARCHAR, "VARCHAR", 0, signed = false, nullable = true))
    mockColumnCount(rsmd, 10)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(err) =>
        println(err)
        fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 10)
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

  it should "make column string" in {
    (new SchemaTools()).makeColumnsString(Nil, StructType(Nil))
    succeed
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

  it should "parse 1D arrays" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)
    val testColDef = TestColumnDef(1, "col1", java.sql.Types.ARRAY, "ARRAY", 0,signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef)
    mockColumnCount(rsmd, 1)

    val tbName = tablename.name.replaceAll("\"", "")
    val childTypeInfo = TestVerticaTypeDef("", 6, java.sql.Types.DECIMAL, "childTypeName", 0, 0)
    val verticaArrayId = VERTICA_NATIVE_ARRAY_BASE_ID + childTypeInfo.verticaTypeId
    val rootTypeDef = TestVerticaTypeDef("", verticaArrayId, java.sql.Types.ARRAY, "rootTypeName", 5, 2,List(childTypeInfo))

    // Query column type info
    VerticaTableTests.mockGetColumnInfo(testColDef.name, tbName, "", rootTypeDef.verticaTypeId, rootTypeDef.typeName, jdbcLayer, rootTypeDef.size, rootTypeDef.scale)

    // Query root type info
    val (_, rootRs) = VerticaTableTests.mockGetTypeInfo(verticaArrayId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(rootTypeDef.verticaTypeId, rootTypeDef.typeName, rootTypeDef.jdbcTypeId, rootRs)
    (rootRs.next _).expects().returning(false)

    // Query element type info
    val (_, elementRs) = VerticaTableTests.mockGetTypeInfo(childTypeInfo.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(childTypeInfo.verticaTypeId, childTypeInfo.typeName, childTypeInfo.jdbcTypeId, elementRs)
    (elementRs.next _).expects().returning(false)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(error) =>  fail(error.getFullContext)
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields(0).dataType.isInstanceOf[ArrayType])
        assert(fields(0).dataType.asInstanceOf[ArrayType]
          .elementType.isInstanceOf[DecimalType])
        assert(!fields(0).metadata.getBoolean(MetadataKey.IS_VERTICA_SET))
        assert(fields(0).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[DecimalType].precision == 5)
        assert(fields(0).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[DecimalType].scale == 2)
    }
  }

  it should "parse array[binary]" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)
    val testColDef = TestColumnDef(1, "col1", java.sql.Types.ARRAY, "ARRAY", 0,signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef)
    mockColumnCount(rsmd, 1)

    val tbName = tablename.name.replaceAll("\"", "")
    val childTypeInfo = TestVerticaTypeDef("", 22, java.sql.Types.BINARY, "childTypeName", 0, 0)
    val verticaArrayId = VERTICA_NATIVE_ARRAY_BASE_ID + childTypeInfo.verticaTypeId
    val rootTypeInfo = TestVerticaTypeDef("", verticaArrayId, java.sql.Types.ARRAY, "rootTypeName", 5, 2,List(childTypeInfo))

    // Query column type info
    VerticaTableTests.mockGetColumnInfo(testColDef.name, tbName, "", rootTypeInfo.verticaTypeId, rootTypeInfo.typeName, jdbcLayer, rootTypeInfo.size, rootTypeInfo.scale)

    // Query root type info
    val (_, rootRs) = VerticaTableTests.mockGetTypeInfo(verticaArrayId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(rootTypeInfo.verticaTypeId, rootTypeInfo.typeName, rootTypeInfo.jdbcTypeId, rootRs)
    (rootRs.next _).expects().returning(false)

    // Query element type info
    val (_, elementRs) = VerticaTableTests.mockGetTypeInfo(117, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(childTypeInfo.verticaTypeId, childTypeInfo.typeName, childTypeInfo.jdbcTypeId, elementRs)
    (elementRs.next _).expects().returning(false)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(error) =>  fail(error.getFullContext)
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields(0).dataType.isInstanceOf[ArrayType])
        assert(fields(0).dataType.asInstanceOf[ArrayType]
          .elementType.isInstanceOf[BinaryType])
        assert(!fields(0).metadata.getBoolean(MetadataKey.IS_VERTICA_SET))
    }
  }

  it should "query columns in a table under a schema space" in {
    val schema = "schema"
    val tableSource = this.tablename.copy(dbschema = Some(schema))
    val (jdbcLayer, mockRs, rsmd) = mockJdbcDeps(tableSource)
    val testColDef = TestColumnDef(1, "col1", java.sql.Types.ARRAY, "ARRAY", 0, signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef)
    mockColumnCount(rsmd, 1)

    val tbName = tableSource.name.replaceAll("\"", "")
    val childTypeInfo = TestVerticaTypeDef("", 6, java.sql.Types.BIGINT, "childTypeName", 0, 0)
    val verticaArrayId = ComplexTypesSchemaTools.VERTICA_NATIVE_ARRAY_BASE_ID + childTypeInfo.verticaTypeId
    val rootTypeDef = TestVerticaTypeDef("", verticaArrayId, java.sql.Types.ARRAY, "rootTypeName", 0, 0, List(childTypeInfo))

    // Query column type info
    VerticaTableTests.mockGetColumnInfo(testColDef.name, tbName, schema, rootTypeDef.verticaTypeId, rootTypeDef.typeName, jdbcLayer)

    // Query root type info
    val (_, typesRs) = VerticaTableTests.mockGetTypeInfo(rootTypeDef.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(rootTypeDef.verticaTypeId, rootTypeDef.typeName, rootTypeDef.jdbcTypeId, typesRs)
    (typesRs.next _).expects().returning(false)

    // Query element type info
    val (_, elementRs) = VerticaTableTests.mockGetTypeInfo(childTypeInfo.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(childTypeInfo.verticaTypeId, childTypeInfo.typeName, childTypeInfo.jdbcTypeId, elementRs)
    (elementRs.next _).expects().returning(false)

    (new SchemaTools).readSchema(jdbcLayer, tableSource) match {
      case Left(error) =>  fail(error.getFullContext)
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields(0).dataType.isInstanceOf[ArrayType])
        assert(fields(0).dataType.asInstanceOf[ArrayType]
          .elementType.isInstanceOf[LongType])
        assert(!fields(0).metadata.getBoolean(MetadataKey.IS_VERTICA_SET))
    }
  }

  it should "correctly detect Vertica Set" in {
    val (jdbcLayer, mockRs, rsmd) = mockJdbcDeps(tablename)
    val testColDef = TestColumnDef(1, "col1", java.sql.Types.ARRAY, "ARRAY", 0, signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef)
    mockColumnCount(rsmd, 1)

    val tbName = tablename.name.replaceAll("\"", "")
    val childTypeInfo = TestVerticaTypeDef("", 6, java.sql.Types.BIGINT, "childTypeName", 0, 0)
    val verticaArrayId = ComplexTypesSchemaTools.VERTICA_SET_BASE_ID + childTypeInfo.verticaTypeId
    val rootTypeDef = TestVerticaTypeDef("", verticaArrayId, java.sql.Types.ARRAY, "rootTypeName", 0, 0, List(childTypeInfo))

    // Query column type info
    VerticaTableTests.mockGetColumnInfo(testColDef.name, tbName, "", rootTypeDef.verticaTypeId, rootTypeDef.typeName, jdbcLayer)

    // Query root type info
    val (_, rootRs) = VerticaTableTests.mockGetTypeInfo(rootTypeDef.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(rootTypeDef.verticaTypeId, rootTypeDef.typeName, rootTypeDef.jdbcTypeId, rootRs)
    (rootRs.next _).expects().returning(false)

    // Query element type info
    val (_, typesRs) = VerticaTableTests.mockGetTypeInfo(childTypeInfo.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(childTypeInfo.verticaTypeId, childTypeInfo.typeName, childTypeInfo.jdbcTypeId, typesRs)
    (typesRs.next _).expects().returning(false)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(error) => fail(error.getFullContext)
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields(0).dataType.isInstanceOf[ArrayType])
        assert(fields(0).dataType.asInstanceOf[ArrayType]
          .elementType.isInstanceOf[LongType])
        assert(fields(0).metadata.getBoolean(MetadataKey.IS_VERTICA_SET))
    }
  }

  it should "parse Vertica Row with primitive fields" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)
    val testColDef = TestColumnDef(1, "col1", java.sql.Types.STRUCT, "ROW", 0, signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef)
    mockColumnCount(rsmd, 1)

    val tbName = tablename.name.replaceAll("\"", "")
    val field1 = TestVerticaTypeDef("f1", 6, java.sql.Types.BIGINT, "field1Type", 0, 0, List())
    val field2 = TestVerticaTypeDef("f2", 7, java.sql.Types.VARCHAR, "field2Type", 0, 0, List())
    val rootTypeDef = TestVerticaTypeDef("root", 123456789L, java.sql.Types.STRUCT, "row", 0, 0, List(field1, field2))

    // Query column type info
    VerticaTableTests.mockGetColumnInfo(testColDef.name, tbName, "", rootTypeDef.verticaTypeId, rootTypeDef.typeName, jdbcLayer)

    // Query Row Def
    val (_, structRs) = VerticaTableTests.mockGetComplexTypeInfo(rootTypeDef.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockComplexTypeInfoResult(rootTypeDef, field1, structRs)
    VerticaTableTests.mockComplexTypeInfoResult(rootTypeDef, field2, structRs)
    (structRs.next _).expects().returning(false)

    val (_, field1Rs) = VerticaTableTests.mockGetTypeInfo(field1.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(field1, field1Rs)
    (field1Rs.next _).expects().returning(false)

    val (_, field2Rs) = VerticaTableTests.mockGetTypeInfo(field2.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(field2, field2Rs)
    (field2Rs.next _).expects().returning(false)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(error) =>  fail(error.getFullContext)
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields.head.dataType.isInstanceOf[StructType])
        // Struct parsing is not yet supported
        assert(fields.head.dataType.asInstanceOf[StructType].fields.nonEmpty)
        assert(fields.head.dataType.asInstanceOf[StructType].fields(0).dataType.isInstanceOf[LongType])
        assert(fields.head.dataType.asInstanceOf[StructType].fields(1).dataType.isInstanceOf[StringType])
    }
  }

  it should "parse Vertica Row[Array]" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)
    val testColDef = TestColumnDef(1, "col1", java.sql.Types.STRUCT, "ROW", 0, signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef)
    mockColumnCount(rsmd, 1)

    val tbName = tablename.name.replaceAll("\"", "")
    val element = TestVerticaTypeDef("element", 7, java.sql.Types.VARCHAR, "field2Type", 0, 0, List())
    val innerArray = TestVerticaTypeDef("innerArray", 123456789, java.sql.Types.ARRAY, "array", 0, 0, List(element))
    val arrayField = TestVerticaTypeDef("array", 222222222, java.sql.Types.ARRAY, "array", 0, 0, List(innerArray))
    val rootTypeDef = TestVerticaTypeDef("root", 111111111, java.sql.Types.STRUCT, "row", 0, 0, List(arrayField))

    // Query column type info
    VerticaTableTests.mockGetColumnInfo(testColDef.name, tbName, "", rootTypeDef.verticaTypeId, rootTypeDef.typeName, jdbcLayer)

    // Query Row Def
    val (_, structRs) = VerticaTableTests.mockGetComplexTypeInfo(rootTypeDef.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockComplexTypeInfoResult(rootTypeDef, arrayField, "_ct_array" ,structRs)
    // VerticaTableTests.mockComplexTypeInfoResult(rootTypeDef, field2, structRs)
    (structRs.next _).expects().returning(false)

    // Query array type
    val (_, arrayRs) = VerticaTableTests.mockGetComplexTypeInfo(arrayField.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockComplexTypeInfoResult(arrayField, innerArray, "_ct_array", arrayRs)
    (arrayRs.next _).expects().returning(false)

    // Query inner array type
    val (_, arrayRs3) = VerticaTableTests.mockGetComplexTypeInfo(innerArray.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockComplexTypeInfoResult(innerArray, element, arrayRs3)
    (arrayRs3.next _).expects().returning(false)

    // Query element type
    val (_, elementRs) = VerticaTableTests.mockGetTypeInfo(element.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(element, elementRs)
    (elementRs.next _).expects().returning(false)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(error) =>  fail(error.getFullContext)
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 1)
        assert(fields.head.dataType.isInstanceOf[StructType])
        // Struct parsing is not yet supported
        assert(fields.head.dataType.asInstanceOf[StructType].fields.nonEmpty)
        assert(fields.head.dataType.asInstanceOf[StructType].fields(0).dataType.isInstanceOf[ArrayType])
    }
  }

  it should "error on type not found" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)
    val testColDef = TestColumnDef(1, "col1", java.sql.Types.ARRAY, "ARRAY", 0, signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef)
    mockColumnCount(rsmd, 1)

    val tbName = tablename.name.replaceAll("\"", "")
    val childTypeInfo = TestVerticaTypeDef("", 6, java.sql.Types.BIGINT, "childTypeName", 0, 0)
    val verticaArrayId = VERTICA_NATIVE_ARRAY_BASE_ID + childTypeInfo.verticaTypeId
    val rootTypeDef = TestVerticaTypeDef("", verticaArrayId, java.sql.Types.ARRAY, "rootTypeName", 0, 0, List(childTypeInfo))

    // Query column type info
    VerticaTableTests.mockGetColumnInfo(testColDef.name, tbName, "", rootTypeDef.verticaTypeId, rootTypeDef.typeName, jdbcLayer)

    // Query root type info
    val (_, rootRs) = VerticaTableTests.mockGetTypeInfo(rootTypeDef.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(rootTypeDef.verticaTypeId, rootTypeDef.typeName, rootTypeDef.jdbcTypeId, rootRs)
    (rootRs.next _).expects().returning(false)

    // Query element type info but returns an empty result set
    val (_, typesRs) = VerticaTableTests.mockGetTypeInfo(childTypeInfo.verticaTypeId, jdbcLayer)
    (typesRs.next _).expects().returning(false)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(err) => succeed
      case Right(schema) => fail(schema.toString)
    }
  }

  it should "parse nested array" in {
    val (jdbcLayer,_, rsmd) = mockJdbcDeps(tablename)
    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.BIGINT, "BIGINT", 0, signed = true, nullable = true))
    val nestedArrayColDef = TestColumnDef(2, "col2", java.sql.Types.ARRAY, "ARRAY", 0, signed = true, nullable = true)
    mockColumnMetadata(rsmd, nestedArrayColDef)
    mockColumnCount(rsmd, 2)

    val tbName = tablename.name.replaceAll("\"", "")
    val leaf = TestVerticaTypeDef("", 3, java.sql.Types.DECIMAL, "int", 5, 2)
    val childArray = TestVerticaTypeDef("", 2000000L, java.sql.Types.ARRAY, "array", 0, 0, List(leaf))
    val array = TestVerticaTypeDef("", 1000000L, java.sql.Types.ARRAY, "array", 0, 0, List(childArray))

    // Query column type info
    VerticaTableTests.mockGetColumnInfo(nestedArrayColDef.name, tbName, "", array.verticaTypeId, array.typeName, jdbcLayer)

    // Query root type info
    val (_, rootRs) = VerticaTableTests.mockGetComplexTypeInfo(array.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockComplexTypeInfoResult(array, childArray, "_ct_array", rootRs)
    (rootRs.next _).expects().returning(false)

    // Query child type info
    val (_, childRs) = VerticaTableTests.mockGetComplexTypeInfo(childArray.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockComplexTypeInfoResult(childArray, leaf, "int", childRs)
    (childRs.next _).expects().returning(false)

    // Query leaf type info
    val (_, leafRs) = VerticaTableTests.mockGetTypeInfo(childArray.children.head.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(leaf.verticaTypeId, leaf.typeName, leaf.jdbcTypeId, leafRs)
    (leafRs.next _).expects().returning(false)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(error) =>
        println(error)
        fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 2)
        assert(fields(0).dataType.isInstanceOf[LongType])

        val arrayDef = fields(1).dataType
        assert(arrayDef.isInstanceOf[ArrayType])
        val element1 = arrayDef.asInstanceOf[ArrayType].elementType
        assert(element1.isInstanceOf[ArrayType])
        val element2 = element1.asInstanceOf[ArrayType].elementType
        assert(element2.isInstanceOf[DecimalType])
        assert(!fields(1).metadata.getBoolean(MetadataKey.IS_VERTICA_SET))
        assert(element2.asInstanceOf[DecimalType].precision == 5)
        assert(element2.asInstanceOf[DecimalType].scale == 2)
    }
  }

  it should "parse array array[row]" in {
    val (jdbcLayer,_, rsmd) = mockJdbcDeps(tablename)
    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.BIGINT, "BIGINT", 0, signed = true, nullable = true))
    val nestedArrayColDef = TestColumnDef(2, "col2", java.sql.Types.ARRAY, "ARRAY", 0, signed = true, nullable = true)
    mockColumnMetadata(rsmd, nestedArrayColDef)
    mockColumnCount(rsmd, 2)

    val tbName = tablename.name.replaceAll("\"", "")
    val field1 = TestVerticaTypeDef("key", 10, java.sql.Types.VARCHAR, "leafType", 5, 2)
    val field2 = TestVerticaTypeDef("value", 6, java.sql.Types.BIGINT, "leafType", 5, 2)
    val struct = TestVerticaTypeDef("", 2000000L, java.sql.Types.STRUCT, "row", 0, 0, List(field1, field2))
    val array = TestVerticaTypeDef("", 1000000L, java.sql.Types.ARRAY, "array", 0, 0, List(struct))

    // Query column type info
    VerticaTableTests.mockGetColumnInfo(nestedArrayColDef.name, tbName, "", array.verticaTypeId, array.typeName, jdbcLayer)

    // Query array type info
    val (_, arrayRs) = VerticaTableTests.mockGetComplexTypeInfo(array.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockComplexTypeInfoResult(array, struct, "_ct_struct", arrayRs)
    (arrayRs.next _).expects().returning(false)

    // Query struct type info
    val (_, structRs) = VerticaTableTests.mockGetComplexTypeInfo(struct.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockComplexTypeInfoResult(struct, field1, structRs)
    VerticaTableTests.mockComplexTypeInfoResult(struct, field2, structRs)
    (structRs.next _).expects().returning(false)

    // Query key type info
    val (_, field1Rs) = VerticaTableTests.mockGetTypeInfo(field1.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(field1, field1Rs)
    (field1Rs.next _).expects().returning(false)

    // Query value type info
    val (_, field2Rs) = VerticaTableTests.mockGetTypeInfo(field2.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(field2, field2Rs)
    (field2Rs.next _).expects().returning(false)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(error) =>
        println(error)
        fail
      case Right(schema) =>
        val fields = schema.fields
        assert(fields.length == 2)
        assert(fields(0).dataType.isInstanceOf[LongType])

        val arrayType = fields(1).dataType
        assert(arrayType.isInstanceOf[ArrayType])
        assert(!fields(1).metadata.getBoolean(MetadataKey.IS_VERTICA_SET))
        val structType = arrayType.asInstanceOf[ArrayType].elementType
        assert(structType.isInstanceOf[StructType])
        val keyType = structType.asInstanceOf[StructType].fields(0).dataType
        assert(keyType.isInstanceOf[StringType])
        val valueType = structType.asInstanceOf[StructType].fields(1).dataType
        assert(valueType.isInstanceOf[LongType])
    }
  }

  it should "error on unsupported types" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    mockColumnMetadata(rsmd, TestColumnDef(1, "col2", java.sql.Types.DATALINK, "DATALINK", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(2, "col3", java.sql.Types.DISTINCT, "DISTINCT", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(3, "col4", java.sql.Types.JAVA_OBJECT, "JAVA_OBJECT", 0, signed = true, nullable = true))
    mockColumnMetadata(rsmd, TestColumnDef(4, "col5", java.sql.Types.NULL, "NULL", 0, signed = true, nullable = true))
    mockColumnCount(rsmd, 4)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(err) => assert(err.getUnderlyingError match {
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
        assert(err.getUnderlyingError match {
          case ErrorList(errors) => errors.size == 2
          case _ => false
        })
        assert(err.getUnderlyingError match {
          case ErrorList(errors) => errors.head match {
            case MissingSqlConversionError(_, _) => true
            case _ => false
          }
          case _ => false
        })
        assert(err.getUnderlyingError match {
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
        err.getUnderlyingError match {
          case ErrorList(errors) => errors.toList.foreach(error => assert(error.getUserMessage ==
            "Could not find conversion for unsupported SQL type: invalid-type" +
            "\nSQL type value: 50000"))
          case _ => false
        }
      case Right(_) => fail
    }
  }

  it should "provide a good error message when trying to convert invalid Spark types to SQL types" in {
    (new SchemaTools).getVerticaTypeFromSparkType(CharType(0), 0, 0, Metadata.empty) match {
      case Left(err) =>
        err.getUnderlyingError match {
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
        assert(err.getUnderlyingError match {
          case JdbcSchemaError(_) => true
          case _ => false
        })
      case Right(_) => fail
    }
  }

  it should "Convert basic spark types to vertica types" in {
    val schemaTools = new SchemaTools

    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.BinaryType, 1, 0, Metadata.empty) == Right("VARBINARY(65000)"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.BooleanType, 1, 0, Metadata.empty) == Right("BOOLEAN"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.ByteType, 1, 0, Metadata.empty) == Right("TINYINT"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.DateType, 1, 0, Metadata.empty ) == Right("DATE"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.CalendarIntervalType, 1, 0, Metadata.empty) == Right("INTERVAL"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.DoubleType, 1 , 0, Metadata.empty) == Right("DOUBLE PRECISION"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.DecimalType(0, 0), 1 , 0, Metadata.empty) == Right("DECIMAL"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.DecimalType(5, 2), 1 , 0, Metadata.empty) == Right("DECIMAL(5, 2)"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.FloatType, 1, 0, Metadata.empty ) == Right("FLOAT"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.IntegerType, 1 , 0, Metadata.empty) == Right("INTEGER"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.LongType, 1 , 0, Metadata.empty) == Right("BIGINT"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.NullType, 1 , 0, Metadata.empty) == Right("null"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.ShortType, 1 , 0, Metadata.empty) == Right("SMALLINT"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.TimestampType, 1 , 0, Metadata.empty) == Right("TIMESTAMP"))
  }

  it should "Convert Spark Sql array to Vertica array" in {
    val schemaTools = new SchemaTools
    assert(schemaTools.getVerticaTypeFromSparkType(ArrayType(StringType), 0, 0, Metadata.empty) == Right("ARRAY[VARCHAR(0)]"))
    assert(schemaTools.getVerticaTypeFromSparkType(ArrayType(StringType), 0, 2, Metadata.empty) == Right("ARRAY[VARCHAR(0),2]"))
    assert(schemaTools.getVerticaTypeFromSparkType(ArrayType(ArrayType(StringType)), 100, 2, Metadata.empty) == Right("ARRAY[ARRAY[VARCHAR(100),2],2]"))
  }

  it should "Convert Spark Set to Vertica Set" in {
    val metadata = new MetadataBuilder().putBoolean(MetadataKey.IS_VERTICA_SET, true).build
    val schemaTools = new SchemaTools
    assert(schemaTools.getVerticaTypeFromSparkType(ArrayType(StringType), 0, 0, metadata) == Right("SET[VARCHAR(0)]"))
    assert(schemaTools.getVerticaTypeFromSparkType(ArrayType(StringType), 0, 2,  metadata) == Right("SET[VARCHAR(0),2]"))
    assert(schemaTools.getVerticaTypeFromSparkType(ArrayType(StringType), 0, 0, Metadata.empty) == Right("ARRAY[VARCHAR(0)]"))
    assert(schemaTools.getVerticaTypeFromSparkType(ArrayType(StringType), 0, 2, Metadata.empty) == Right("ARRAY[VARCHAR(0),2]"))
  }

  it should "Convert struct to Vertica row" in {
    val schemaTools = new SchemaTools
    val primitiveRow = StructType(Array(
      StructField("col1", IntegerType, false, Metadata.empty),
      StructField("col2", IntegerType, false, Metadata.empty)))
    assert(schemaTools.getVerticaTypeFromSparkType(primitiveRow, 0, 0, Metadata.empty)
      == Right("ROW(\"col1\" INTEGER, \"col2\" INTEGER)"))

    val nativeArrayRow = StructType(Array(StructField("col1", ArrayType(IntegerType), true, Metadata.empty)))
    assert(schemaTools.getVerticaTypeFromSparkType(nativeArrayRow, 0, 0, Metadata.empty)
      == Right("ROW(\"col1\" ARRAY[INTEGER])"))

    val nestedRows = StructType(Array(
      StructField("col1", StructType(Array(
        StructField("field1", IntegerType, true, Metadata.empty)
      )), true, Metadata.empty),
    ))
    assert(schemaTools.getVerticaTypeFromSparkType(nestedRows, 0, 0, Metadata.empty)
      == Right("ROW(\"col1\" ROW(\"field1\" INTEGER))"))
  }

  it should "Provide error message on unknown element type conversion to vertica" in {
    (new SchemaTools).getVerticaTypeFromSparkType(ArrayType(CharType(0)),0,0, Metadata.empty) match {
      case Left(err) =>
        err.getUnderlyingError match {
          case ErrorList(errors) => errors.toList.foreach(error => assert(error.getUserMessage ==
            "Could not find conversion for unsupported Spark type: CharType"))
          case _ => false
        }
      case Right(_) => fail
    }
  }

  it should "Convert string types to vertica type properly" in {
    val schemaTools = new SchemaTools

    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.StringType, 1024, 0, Metadata.empty) == Right("VARCHAR(1024)"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.StringType, 5000, 0, Metadata.empty) == Right("VARCHAR(5000)"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.StringType, 65000, 0, Metadata.empty) == Right("VARCHAR(65000)"))
    assert(schemaTools.getVerticaTypeFromSparkType(org.apache.spark.sql.types.StringType, 100000, 0, Metadata.empty) == Right("LONG VARCHAR(100000)"))
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

  it should "Return an updated column type" in {
    val schema = new StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))
    val schemaTools= new SchemaTools
    val col = "\"name\" varchar"
    val colName = "\"name\""
    val updatedField = schemaTools.updateFieldDataType(col, colName, schema, 1024, 0)
    assert(updatedField == "\"name\" VARCHAR(1024)")
  }

  it should "Return the same column type" in {
    val schema = new StructType(Array(StructField("name", StringType)))
    val schemaTools= new SchemaTools
    val col = "\"age\" integer"
    val colName = "\"age\""
    val updatedField = schemaTools.updateFieldDataType(col, colName, schema, 1024, 0)
    assert(updatedField == "\"age\" integer")
  }

  it should "Return an updated column type for an unknown col type" in {
    val schema = new StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))
    val schemaTools= new SchemaTools
    val col = "\"age\" UNKNOWN"
    val colName = "\"age\""
    val updatedField = schemaTools.updateFieldDataType(col, colName, schema, 1024, 0)
    assert(updatedField == "\"age\" INTEGER")
  }

  it should "update field data type to varchar and long varchar" in {
    val schemaTools= new SchemaTools
    val maxLength1 = 65001
    val metadata1 = new MetadataBuilder().putLong("maxlength", maxLength1).build()
    val maxLength2 = 400
    val metadata2 = new MetadataBuilder().putLong("maxlength", maxLength2).build()
    val schema = new StructType(Array(
      StructField("col1", StringType, metadata = metadata1),
      StructField("col2", StringType, metadata = metadata2)
    ))

    val colName = "\"col1\""
    val col = s"$colName VARCHAR($maxLength1)"
    val updatedField = schemaTools.updateFieldDataType(col, colName, schema, 1024, 0)
    assert(updatedField == s"$colName long varchar($maxLength1)")

    val col2Name = "\"col2\""
    val col2 = s"$col2Name VARCHAR($maxLength2)"
    val updatedField2 = schemaTools.updateFieldDataType(col2, col2Name, schema, 1024, 0)
    assert(updatedField2 == s"$col2Name varchar($maxLength2)")
  }

  it should "Return an updated create external table statement" in {
    val schema = new StructType(Array(StructField("date", DateType, nullable = true), StructField("region", IntegerType, nullable = true)))
    val createExternalTableStmt = "create external table \"sales\"(" +
      "\"tx_id\" int," +
      "\"date\" UNKNOWN," +
      "\"region\" varchar" +
      ") as copy from \'/data/\' parquet"
    val schemaTools = new SchemaTools
    schemaTools.inferExternalTableSchema(createExternalTableStmt, schema, "sales", 100, 0) match {
      case Left(err) =>
        fail(err.getFullContext)
      case Right(str) =>
        assert(str == "create external table sales(\"tx_id\" int,\"date\" DATE,\"region\" INTEGER) as copy from \'/data/\' parquet")
    }
  }

  it should "infer external table schema with nested parenthesis in create statement" in {
    val schema = new StructType(Array(StructField("col1", DecimalType(10, 4), nullable = true), StructField("col2", IntegerType, nullable = true)))
    val createExternalTableStmt = "create external table \"sales\"(\n" +
      "\"col1\" numeric(10,4),\n" +
      "\"col2\" int" +
      ") as copy from \'/data/\' parquet(path=\"./\")"
    val schemaTools = new SchemaTools
    schemaTools.inferExternalTableSchema(createExternalTableStmt, schema, "sales", 100, 0) match {
      case Left(err) =>
        fail(err.getFullContext)
      case Right(str) =>
        assert(str == "create external table sales(\"col1\" DECIMAL(10, 4),\"col2\" INTEGER) as copy from \'/data/\' parquet(path=\"./\")")
    }
  }

  it should "Return an error if partial schema doesn't match partitioned columns" in {
    val schema = new StructType(Array(StructField("foo", DateType, nullable = true), StructField("bar", StringType, nullable = true)))
    val createExternalTableStmt = "create external table \"sales\"(" +
      "\"tx_id\" int," +
      "\"date\" UNKNOWN," +
      "\"region\" UNKNOWN" +
      ") as copy from \'/data/\' parquet"
    val schemaTools = new SchemaTools
    schemaTools.inferExternalTableSchema(createExternalTableStmt, schema, "sales", 100, 0) match {
      case Left(err) => err.isInstanceOf[UnknownColumnTypesError]
      case Right(str) => fail
    }
  }

  private val primitiveCol = StructField("col1", IntegerType)
  private val nativeArrayCol = StructField("col1", ArrayType(IntegerType))
  private val complexArrayCol = StructField("col1", ArrayType(ArrayType(IntegerType)))
  private val mapCol = StructField("col1", MapType(IntegerType, IntegerType))
  private val rowCol = StructField("col1", StructType(Array(StructField("col2", IntegerType))))

  it should "Error when schema only contains complex types columns" in {
    val schemaTools = new SchemaTools()

    val failingSchema = StructType(Array(complexArrayCol, mapCol, rowCol))
    assert(schemaTools.checkValidTableSchema(failingSchema)
      == Left(InvalidTableSchemaComplexType()))

    val passingSchema1 = StructType(Array(primitiveCol, complexArrayCol, mapCol, rowCol))
    assert(schemaTools.checkValidTableSchema(passingSchema1) == Right())

    val passingSchema2 = StructType(Array(nativeArrayCol, complexArrayCol, mapCol, rowCol))
    assert(schemaTools.checkValidTableSchema(passingSchema2) == Right())

    val passingSchema3 = StructType(Array(nativeArrayCol, primitiveCol))
    assert(schemaTools.checkValidTableSchema(passingSchema3) == Right())
  }

  it should "Error on empty schema" in {
    val emptySchema = new StructType(Array())
    assert(new SchemaTools().checkValidTableSchema(emptySchema) == Left(EmptySchemaError()))
  }


  it should "Cast Vertica SET to ARRAY in column string" in {
    val requiredSchema = StructType(Nil)

    val typeName="COL_TYPE_NAME"
    val elementDef = ColumnDef("element", java.sql.Types.BIGINT, typeName, 0, 0, true, false, Metadata.empty)

    val colName = "col1"
    val metadata = new MetadataBuilder().putBoolean(MetadataKey.IS_VERTICA_SET, true).build()
    val colDef = List(ColumnDef(colName, java.sql.Types.ARRAY, "SET", 0, 0, true, false, metadata, List(elementDef)))

    val colsString = new SchemaTools().makeColumnsString(colDef, requiredSchema)
    println(colsString)
    val expected = s"""("$colName"::ARRAY[$typeName]) as "$colName""""
    println(expected)
    assert(colsString.trim().equals(expected))
  }

  it should "Convert Spark MapType to Vertica Map" in {
    val schemaTools = new SchemaTools
    val expected = s"MAP<VARCHAR(1024), INTEGER>"

    val mapType = new MapType(StringType, IntegerType, true)
    assert(schemaTools.getVerticaTypeFromSparkType(mapType, 1024, 0, Metadata.empty)
      == Right(expected))
  }

  it should "Error on converting MapType that uses complex type" in {
    val schemaTools = new SchemaTools

    val mapType1 = new MapType(ArrayType(StringType), IntegerType, true)
    schemaTools.getVerticaTypeFromSparkType(mapType1, 1024, 0, Metadata.empty) match {
      case Right(_) => fail("Expected schema error")
      case Left(error) => assert(error.isInstanceOf[MapDataTypeConversionError])
    }

    val mapType3 = new MapType(IntegerType, ArrayType(StringType), true)
    schemaTools.getVerticaTypeFromSparkType(mapType3, 1024, 0, Metadata.empty) match {
      case Right(_) => fail("Expected schema error")
      case Left(error) => assert(error.isInstanceOf[MapDataTypeConversionError])
    }

    val mapType2 = new MapType(MapType(IntegerType, IntegerType), ArrayType(IntegerType), true)
    schemaTools.getVerticaTypeFromSparkType(mapType2, 1024, 0, Metadata.empty) match {
      case Right(_) => fail("Expected schema error")
      case Left(error) => assert(error.isInstanceOf[MapDataTypeConversionError])
        println(error.getFullContext)
    }
  }

  it should "Check for map containing complex type" in {
    val schemaTools = new SchemaTools

    val mapType1 = new MapType(ArrayType(StringType), IntegerType, true)
    val mapType2 = new MapType(StringType, ArrayType(IntegerType), true)
    val schema2 = StructType(Array(
      StructField("col1", IntegerType),
      StructField("col2", mapType2),
      StructField("col3", mapType1)
    ))

    schemaTools.checkValidTableSchema(schema2) match {
      case Right(_) => fail("Expected connector error")
      case Left(error) => error match {
        case ErrorList(errors) =>
          assert(errors.length == 2)
          assert(errors.head.isInstanceOf[InvalidMapSchemaError])
          assert(errors.tail.head.isInstanceOf[InvalidMapSchemaError])
        case ConnectionError(_) =>
      }
    }
  }

  it should "add schema to query" in {
    val query = "SELECT * From dftest join    dftest2 on dftest.a = dftest2.b where x = 1"
    assert(new SchemaTools().addDbSchemaToQuery(query, Some("schema"))
      == "SELECT * From schema.dftest join    dftest2 on dftest.a = dftest2.b where x = 1")
  }

  it should "not add schema to query with sub-query in FROM clause" in {
    val query = "SELECT * from (select * from) where x = 1"
    assert(new SchemaTools().addDbSchemaToQuery(query, Some("schema")) == query)
  }

  it should "not add schema to query when already present" in {
    val query = "SELECT * from test.dftest where x = 1"
    assert(new SchemaTools().addDbSchemaToQuery(query, Some("schema")) == query)
  }

  it should "not add schema to query with schema and database are already present" in {
    val query = "SELECT * from db.test.dftest where x = 1"
    assert(new SchemaTools().addDbSchemaToQuery(query, Some("schema")) == query)
  }

  it should "add schema to query with literal table name" in {
    val query = "SELECT * From \"dftest\" join dftest2 on dftest.a = dftest2.b where x = 1"
    assert(new SchemaTools().addDbSchemaToQuery(query, Some("schema"))
      == "SELECT * From schema.\"dftest\" join dftest2 on dftest.a = dftest2.b where x = 1")
  }

  it should "add schema to query with literal table name \"df.test\" " in {
    val query = "SELECT * From \"df.test\" join dftest2 on dftest.a = dftest2.b where x = 1"
    assert(new SchemaTools().addDbSchemaToQuery(query, Some("schema"))
      == "SELECT * From schema.\"df.test\" join dftest2 on dftest.a = dftest2.b where x = 1")
  }

  it should "not add schema to query with FROM source \"test.schema\".\"dftest\" " in {
    val query2 = "SELECT * From \"test.schema\".\"df.test\" join dftest2 on dftest.a = dftest2.b where x = 1"
    assert(new SchemaTools().addDbSchemaToQuery(query2, Some("schema")) == query2)
  }

  it should "not add schema to query with literal table name, schema, and database" in {
    val query2 = "SELECT * From \"data.base\".\"test\".\"df.test\" join dftest2 on dftest.a = dftest2.b where x = 1"
    assert(new SchemaTools().addDbSchemaToQuery(query2, Some("schema")) == query2)
  }

  it should "not add schema to query with some literal table name, schema, and database" in {
    val query = "SELECT * From database.\"schema\".dftest join dftest2 on dftest.a = dftest2.b where x = 1"
    assert(new SchemaTools().addDbSchemaToQuery(query, Some("schema")) == query)
  }

  it should "build column def string with empty column name containing no quotations" in {
    val schema = StructType(Array(StructField("", IntegerType)))

    new SchemaTools().makeTableColumnDefs(schema, 0, 0) match {
      case Left(e) => fail("Expected to succeed")
      case Right(str) =>
        assert(str == " (INTEGER)")
    }
  }

  it should "build row column def with empty field name" in {
    val schema = StructType(Array(
      StructField("col1", StructType(Array(
        StructField("", IntegerType),
        StructField("", StringType),
        StructField("cat", IntegerType),
      )))
    ))

    new SchemaTools().makeTableColumnDefs(schema, 0, 0) match {
      case Left(e) => fail("Expected to succeed")
      case Right(str) =>
        assert(str == " (\"col1\" ROW(INTEGER, VARCHAR(0), \"cat\" INTEGER))")
    }
  }

  it should "error on empty column name" in {
    val schema = StructType(Array(
      StructField("col1", StructType(Array(
        StructField("", IntegerType),
        StructField("cat", IntegerType),
      ))),
      StructField("", StringType)
    ))

    new SchemaTools().checkValidTableSchema(schema) match {
      case Left(exp) => assert(exp.isInstanceOf[BlankColumnNamesError])
      case Right(_) => fail("expected to fail")
    }
  }

  it should "error on names with only white space" in {
    val schema = StructType(Array(
      StructField("col1", StructType(Array(
        StructField("", IntegerType),
        StructField("cat", IntegerType),
      ))),
      StructField("   ", StringType)
    ))

    new SchemaTools().checkValidTableSchema(schema) match {
      case Left(exp) => assert(exp.isInstanceOf[BlankColumnNamesError])
      case Right(_) => fail("expected to fail")
    }
  }
}
// For package private access without instantiation
object SchemaToolsTests extends SchemaToolsTests