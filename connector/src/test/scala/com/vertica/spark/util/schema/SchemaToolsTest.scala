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

import com.vertica.spark.config.{TableName, TableQuery}
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

  private def mockJdbcDeps(tablename: TableName): (JdbcLayerInterface, ResultSet, ResultSetMetaData) = {
    val jdbcLayer = mock[JdbcLayerInterface]
    val resultSet = mock[ResultSet]
    val rsmd = mock[ResultSetMetaData]

    (jdbcLayer.query _).expects("SELECT * FROM " + tablename.getFullTableName + " WHERE 1=0", *).returning(Right(resultSet))
    (resultSet.getMetaData _).expects().returning(rsmd)
    (resultSet.close _).expects()

    (jdbcLayer, resultSet, rsmd)

  }

  private def mockJdbcDepsQuery(query: TableQuery): (JdbcLayerInterface, ResultSet, ResultSetMetaData) = {
    val jdbcLayer = mock[JdbcLayerInterface]
    val resultSet = mock[ResultSet]
    val rsmd = mock[ResultSetMetaData]

    (jdbcLayer.query _).expects("SELECT * FROM (" + query.query + ") AS x WHERE 1=0", *).returning(Right(resultSet))
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
    val query = TableQuery("SELECT * FROM t WHERE a > 1", "")
    val (jdbcLayer, _, rsmd) = mockJdbcDepsQuery(query)

    // Schema
    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.REAL, "REAL", 32, signed = false, nullable = true))
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, query) match {
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
    val (jdbcLayer, mockRs, rsmd) = mockJdbcDeps(tablename)
    val testColDef = TestColumnDef(1, "col1", java.sql.Types.ARRAY, "ARRAY", 0, signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef)

    val verticaArrayType = 1506
    val tableName = tablename.getFullTableName.replace("\"", "")
    mockQueryColumns(tableName, testColDef.name,verticaArrayType, jdbcLayer)
    val mockRs1 = mockQueryTypes(verticaArrayType, hasData = true,jdbcLayer)
    (mockRs1.getLong: (String) => Long).expects("jdbc_type").returns(java.sql.Types.BIGINT)
    (mockRs1.getString: (String)=>String).expects("type_name").returns("Integer")
    mockColumnCount(rsmd, 1)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
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

    val verticaSetType = 2706
    val tableName = tablename.getFullTableName.replace("\"", "")
    mockQueryColumns(tableName, testColDef.name,verticaSetType, jdbcLayer)
    val mockRs1 = mockQueryTypes(verticaSetType, hasData = true,jdbcLayer)
    (mockRs1.getLong: (String) => Long).expects("jdbc_type").returns(java.sql.Types.BIGINT)
    (mockRs1.getString: (String)=>String).expects("type_name").returns("Integer")
    mockColumnCount(rsmd, 1)

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

  it should "error on type not found" in {
    val (jdbcLayer, mockRs, rsmd) = mockJdbcDeps(tablename)
    val testColDef = TestColumnDef(1, "col1", java.sql.Types.ARRAY, "ARRAY", 0, signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef)

    val verticaArrayType = 1506
    val tableName = tablename.getFullTableName.replace("\"", "")
    mockQueryColumns(tableName, testColDef.name,verticaArrayType, jdbcLayer)
    mockColumnCount(rsmd, 1)
    val verticaElementType = verticaArrayType;
    mockQueryTypes(verticaElementType, hasData = false,jdbcLayer)

    (new SchemaTools).readSchema(jdbcLayer, tablename) match {
      case Left(err) => succeed
      case Right(schema) => fail(schema.toString)
    }
  }

  private def mockQueryColumns(tableName: String, colName: String, verticaTypeFound: Long, jdbcLayer: JdbcLayerInterface): Unit = {
    val mockRs = mock[ResultSet]
    val queryColumnDef = s"SELECT data_type_id, data_type FROM columns WHERE table_name='$tableName' AND column_name='$colName'"
    (jdbcLayer.query _)
      .expects(queryColumnDef, *)
      .returns(Right(mockRs))
    (mockRs.next _).expects().returns(true)
    (mockRs.getLong: String => Long).expects("data_type_id").returning(verticaTypeFound)
    (mockRs.getString: String => String).expects("data_type").returning("VerticaTypeString")
    (mockRs.close _).expects()
  }

  private def mockQueryTypes(verticaTypeId: Long, hasData: Boolean, jdbcLayer: JdbcLayerInterface): ResultSet = {
    val mockRs = mock[ResultSet]
    var elementId = verticaTypeId - SchemaTools.VERTICA_NATIVE_ARRAY_BASE_ID
    val isSet = elementId > SchemaTools.VERTICA_PRIMITIVES_MAX_ID
    if (isSet) elementId = verticaTypeId - SchemaTools.VERTICA_SET_BASE_ID
    val isNative = elementId < SchemaTools.VERTICA_PRIMITIVES_MAX_ID
    if(isNative){
      (jdbcLayer.query _)
        .expects(s"SELECT type_id, jdbc_type, type_name FROM types WHERE type_id=$elementId", *)
        .returns(Right(mockRs))
      (mockRs.next _).expects().returns(hasData)
      (mockRs.close _).expects()
    }
    mockRs
  }

  it should "parse nested array" in {
    val (jdbcLayer, mockRs, rsmd) = mockJdbcDeps(tablename)
    mockColumnMetadata(rsmd, TestColumnDef(1, "col1", java.sql.Types.BIGINT, "BIGINT", 0, signed = true, nullable = true))
    val testColDef = TestColumnDef(2, "col2", java.sql.Types.ARRAY, "ARRAY", 0, signed = true, nullable = true)

    mockColumnMetadata(rsmd, testColDef)

    val verticaArrayType = 10000000L
    val tableName = tablename.getFullTableName.replace("\"", "")
    mockQueryColumns(tableName, testColDef.name, verticaArrayType, jdbcLayer)

    val fieldId1 = 10000000L
    val mockRs1 = mockQueryComplexType(verticaArrayType, true, isFieldNative = false, jdbcLayer)
    val field1TypeName = "_ct_" + fieldId1.toString
    (mockRs1.getString: String => String).expects("field_type_name").returns(field1TypeName)
    (mockRs1.getLong: String => Long).expects("field_id").returns(fieldId1)

    val fieldId2 = 6L
    val mockRs2 = mockQueryComplexType(fieldId1, true, isFieldNative = true, jdbcLayer)
    val field2TypeName = fieldId2.toString
    (mockRs2.getString: String => String).expects("field_type_name").returns(field2TypeName)
    (mockRs2.getLong: String => Long).expects("field_id").returns(fieldId2)

    val mockRs3 = mock[ResultSet]
    (jdbcLayer.query _)
      .expects(s"SELECT type_id, jdbc_type, type_name FROM types WHERE type_id=$fieldId2", *)
      .returns(Right(mockRs3))
    (mockRs3.next _).expects().returns(true)
    (mockRs3.close _).expects()
    (mockRs3.getLong: (String) => Long).expects("jdbc_type").returns(java.sql.Types.BIGINT)
    (mockRs3.getString: (String) => String).expects("type_name").returns("Integer")

    mockColumnCount(rsmd, 2)

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
        assert(element2.isInstanceOf[LongType])
        assert(!fields(1).metadata.getBoolean(MetadataKey.IS_VERTICA_SET))
    }
  }

  private def mockQueryComplexType(complexTypeId: Long, hasData: Boolean, isFieldNative: Boolean, jdbcLayer: JdbcLayerInterface) = {
    val mockRs = mock[ResultSet]
    val queryComplexType = s"SELECT field_type_name, type_id ,field_id, numeric_scale FROM complex_types WHERE type_id='$complexTypeId'"
    (jdbcLayer.query _).expects(queryComplexType, *).returns(Right(mockRs))
    (mockRs.next _).expects().returns(hasData)
    (mockRs.close _).expects()
    mockRs
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

  it should "convert Spark Map to Vertica Map" in {
    val schemaTools = new SchemaTools
    assert(schemaTools.getVerticaTypeFromSparkType(MapType(StringType, StringType), 0, 0, Metadata.empty) == Right(s"VARBINARY(65000)"))
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

  it should "Cast Vertica SET to ARRAY in column string" in {
    val requiredSchema = StructType(Nil)

    val typeName="COL_TYPE_NAME"
    val elementDef = ColumnDef("element", java.sql.Types.BIGINT, typeName, 0, 0, true, false, Metadata.empty)

    val colName = "col1"
    val metadata = new MetadataBuilder().putBoolean(MetadataKey.IS_VERTICA_SET, true).build()
    val colDef = List(ColumnDef(colName, java.sql.Types.ARRAY, "SET", 0, 0, true, false, metadata, List(elementDef)))

    val colsString = new SchemaTools().makeColumnsString(colDef, requiredSchema)
    println(colsString)
    val expected = s"($colName::ARRAY[$typeName]) as $colName"
    println(expected)
    assert(colsString.trim().equals(expected))
  }
}
