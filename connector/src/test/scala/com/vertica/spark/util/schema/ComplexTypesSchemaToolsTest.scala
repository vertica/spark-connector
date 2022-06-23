package com.vertica.spark.util.schema

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.query.VerticaTableTests
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class ComplexTypesSchemaToolsTest extends AnyFlatSpec with MockFactory{

  behavior of "ComplexTypeSchemaSupportTest"

  //  Todo: Extract the rest of complex types test in SchemaToolsTests here.

  it should "parse array array[row]" in {
    val jdbcLayer = mock[JdbcLayerInterface]
    val tableName = "tableName"
    val schema = "schema"
    val colName = "colName"
    val key = TestVerticaTypeDef("key", 10, java.sql.Types.VARCHAR, "leafType", 5, 2)
    val value = TestVerticaTypeDef("value", 6, java.sql.Types.BIGINT, "leafType", 5, 2)
    val struct = TestVerticaTypeDef("", 2000000L, java.sql.Types.STRUCT, "row", 0, 0, List(key, value))
    val array = TestVerticaTypeDef("", 1000000L, java.sql.Types.ARRAY, "array", 0, 0, List(struct))
    val rootDef = ColumnDef(colName, array.jdbcTypeId.toInt, array.typeName, array.size, array.size, false, false)

    // Query column type info
    VerticaTableTests.mockGetColumnInfo(colName, tableName, schema, array.verticaTypeId, array.typeName, jdbcLayer)

    // Query array type info
    val (_, arrayRs) = VerticaTableTests.mockGetComplexTypeInfo(array.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockComplexTypeInfoResult(array, struct, arrayRs)
    (arrayRs.next _).expects().returning(false)

    // Query struct type info
    val (_, structRs) = VerticaTableTests.mockGetComplexTypeInfo(struct.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockComplexTypeInfoResult(struct, key, structRs)
    VerticaTableTests.mockComplexTypeInfoResult(struct, value, structRs)
    (structRs.next _).expects().returning(false)

    // Query key type info
    val (_, field1Rs) = VerticaTableTests.mockGetTypeInfo(key.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(key, field1Rs)
    (field1Rs.next _).expects().returning(false)

    // Query value type info
    val (_, field2Rs) = VerticaTableTests.mockGetTypeInfo(value.verticaTypeId, jdbcLayer)
    VerticaTableTests.mockTypeInfoResult(value, field2Rs)
    (field2Rs.next _).expects().returning(false)

    new ComplexTypesSchemaTools().startQueryingVerticaComplexTypes(rootDef, tableName, schema, jdbcLayer) match {
      case Left(error) => fail(error.getFullContext)
      case Right(result) =>
        assert(result.jdbcType == array.jdbcTypeId)
        assert(!result.metadata.getBoolean(MetadataKey.IS_VERTICA_SET))
        val element = result.children.headOption.getOrElse(fail)
        assert(element.jdbcType == struct.jdbcTypeId)
        assert(element.children.length == 2)
        val field1 = element.children.head
        assert(field1.jdbcType == key.jdbcTypeId)
        val field2 = element.children(1)
        assert(field2.jdbcType == value.jdbcTypeId)
    }
  }
}
