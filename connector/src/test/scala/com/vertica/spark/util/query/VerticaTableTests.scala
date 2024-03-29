package com.vertica.spark.util.query

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.{ConnectorError, MultipleQueryResult, QueryResultEmpty}
import com.vertica.spark.util.query.VerticaTableTests.{mockComplexTypeInfoResult, mockGetColumnInfo, mockGetComplexTypeInfo, mockGetTypeInfo, mockTypeInfoResult, mockVerticaTableQuery}
import com.vertica.spark.util.schema.TestVerticaTypeDef
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.ResultSet

object VerticaTableTests extends VerticaTableTests {
  def wrapQuotation(str: String): String = "\"" + str +"\""

  def mockVerticaTableQuery(cols: Seq[String], tableName: String, conditions: String, jdbcLayer: JdbcLayerInterface): (JdbcLayerInterface, ResultSet) = {
    val rs = mock[ResultSet]
    val where = if (conditions.isEmpty) "" else " WHERE " + conditions.trim
    val query = s"SELECT ${cols.map(wrapQuotation).mkString(", ")} FROM ${wrapQuotation(tableName)}$where"
    (jdbcLayer.query _).expects(query, *).returning(Right(rs))
    (rs.close _).expects
    (jdbcLayer, rs)
  }

  def mockGetColumnInfo(colName: String, tableName: String, schema: String, typeId: Long, typeName: String, jdbcLayer: JdbcLayerInterface, precision: Long = 0, scale: Long = 0): (JdbcLayerInterface, ResultSet) = {
    val schemaCond = if(schema.nonEmpty) s" AND table_schema='$schema'" else ""
    val conditions = s"table_name='$tableName'$schemaCond AND column_name='$colName'"
    val (jdbc, rs) = mockVerticaTableQuery(List("data_type_id" , "data_type", "numeric_precision", "numeric_scale"), "columns", conditions, jdbcLayer)

    (rs.next _).expects().returning(true)
    (rs.getLong: String => Long).expects("data_type_id").returning(typeId)
    (rs.getString: String => String).expects("data_type").returning(typeName)
    (rs.getLong: String => Long).expects("numeric_precision").returning(precision)
    (rs.getLong: String => Long).expects("numeric_scale").returning(scale)
    (rs.next _).expects().returning(false)

    (jdbc, rs)
  }

  def mockGetComplexTypeInfo(verticaTypeId: Long, jdbcLayer: JdbcLayerInterface): (JdbcLayerInterface, ResultSet) = {
    val conditions = s"type_id=$verticaTypeId"
    val (jdbc, rs) = mockVerticaTableQuery(List("type_id", "type_name", "field_id", "field_type_name", "numeric_scale", "type_kind", "numeric_precision", "field_name"), "complex_types", conditions, jdbcLayer)
    (jdbc, rs)
  }

  def mockComplexTypeInfoResult(fieldTypeName:String, fieldId: Long, typeId: Long, rs: ResultSet, typeKind: String = "", typeName: String = "", precision: Long = 0, scale: Long = 0, fieldName: String = ""): Unit = {
    (rs.next _).expects().returning(true)
    (rs.getLong: String => Long).expects("type_id").returning(typeId)
    (rs.getString: String => String).expects("type_name").returning(typeName)
    (rs.getLong: String => Long).expects("field_id").returning(fieldId)
    (rs.getString: String => String).expects("field_type_name").returning(fieldTypeName)
    (rs.getLong: String => Long).expects("numeric_scale").returning(scale)
    (rs.getString: String => String).expects("type_kind").returning(typeKind)
    (rs.getLong: String => Long).expects("numeric_precision").returning(precision)
    (rs.getString: String => String).expects("field_name").returning(fieldName)
  }

  def mockComplexTypeInfoResult(parentDef: TestVerticaTypeDef, childDef: TestVerticaTypeDef, rs: ResultSet): Unit = {
    mockComplexTypeInfoResult(childDef.typeName, childDef.verticaTypeId, parentDef.verticaTypeId, rs, parentDef.typeName, parentDef.typeName, parentDef.size, parentDef.scale, childDef.name)
  }

  def mockComplexTypeInfoResult(parentDef: TestVerticaTypeDef, childDef: TestVerticaTypeDef, fieldTypeName: String, rs: ResultSet): Unit = {
    mockComplexTypeInfoResult(fieldTypeName, childDef.verticaTypeId, parentDef.verticaTypeId, rs, parentDef.typeName, parentDef.typeName, childDef.size, childDef.scale, childDef.name)
  }

    def mockGetTypeInfo(verticaTypeId: Long, jdbcLayer: JdbcLayerInterface): (JdbcLayerInterface, ResultSet) = {
    val conditions = s"type_id=$verticaTypeId"
    val (jdbc, rs) = mockVerticaTableQuery(List("type_id", "jdbc_type", "type_name", "max_scale"), "types", conditions, jdbcLayer)
    (jdbc, rs)
  }

  def mockTypeInfoResult(typeId: Long, typeName: String, jdbcType: Long, rs: ResultSet): ResultSet = {
    (rs.next _).expects().returning(true)
    (rs.getLong: String => Long).expects("type_id").returning(typeId)
    (rs.getLong: String => Long).expects("jdbc_type").returning(jdbcType)
    (rs.getString: String => String).expects("type_name").returning(typeName)
    (rs.getLong: String => Long).expects("max_scale").returning(0)
    rs
  }

  def mockTypeInfoResult(typeDef: TestVerticaTypeDef, rs: ResultSet): ResultSet = {
    mockTypeInfoResult(typeDef.verticaTypeId, typeDef.typeName ,typeDef.jdbcTypeId, rs)
  }

}

class VerticaTableTests extends AnyFlatSpec with MockFactory with org.scalatest.OneInstancePerTest {
  val colName = "col1"
  val tableName = "table1"
  val schemaName = "schema1"

  class TestVerticaTable(jdbc: JdbcLayerInterface) extends VerticaTable[Unit](jdbc) {
    override def tableName: String = "testTable"

    override def columns: Seq[String] = List("col1", "col2", "col3")

    override protected def buildRow(rs: ResultSet): Unit = ()

    def testSelectWhereExpectOne(): Either[ConnectorError, Unit] = super.selectWhereExpectOne("")
  }

  it should "error on empty result" in {
    val jdbc = mock[JdbcLayerInterface]
    val testTable = (new TestVerticaTable(jdbc))
    val (_, rs) = mockVerticaTableQuery(testTable.columns, testTable.tableName, "", jdbc)
    (rs.next _).expects().returning(false)

    testTable.testSelectWhereExpectOne() match {
      case Left(value) => assert(value.isInstanceOf[QueryResultEmpty])
      case Right(value) => fail("expected to fail")
    }
  }

  it should "error on multiple results" in {
    val jdbc = mock[JdbcLayerInterface]
    val testTable = (new TestVerticaTable(jdbc))
    val (_, rs) = mockVerticaTableQuery(testTable.columns, testTable.tableName, "", jdbc)
    (rs.next _).expects().returning(true)
    (rs.next _).expects().returning(true)
    (rs.next _).expects().returning(false)

    testTable.testSelectWhereExpectOne() match {
      case Left(value) => assert(value.isInstanceOf[MultipleQueryResult])
        println(value.getFullContext)
      case Right(value) => fail("expected to fail")
    }
  }

  it should "find column info" in {
    val typeId = 1
    val typeName = "typeName"
    val (jdbcLayer, _) = mockGetColumnInfo(colName, tableName, "", typeId, typeName, mock[JdbcLayerInterface], 5, 2)

    new ColumnsTable(jdbcLayer)
      .getColumnInfo(colName, tableName, "") match {
      case Left(_) => fail("Expected to succeed")
      case Right(colInfo) =>
        assert(colInfo.dataTypeName == typeName)
        assert(colInfo.verticaType == typeId)
        assert(colInfo.precision == 5)
        assert(colInfo.scale == 2)
    }
  }

  it should "find column info with schema" in {
    val typeId = 1
    val typeName = "typeName"
    val (jdbcLayer, _) = mockGetColumnInfo(colName, tableName, schemaName, typeId, typeName, mock[JdbcLayerInterface])

    new ColumnsTable(jdbcLayer)
      .getColumnInfo(colName, tableName, schemaName) match {
      case Left(_) => fail("Expected to succeed")
      case Right(colInfo) =>
        assert(colInfo.dataTypeName == typeName)
        assert(colInfo.verticaType == typeId)
    }
  }

  it should "get complex types info" in {
    val verticaTypeId = 1
    val fieldTypeName = "fieldTypeName"
    val typeId = 1000000
    val fieldId = 2000000
    val typeKind = "typeKind"
    val typeName = "typeName"
    val scale = 2
    val precision = 5
    val (jdbc, rs) = mockGetComplexTypeInfo(verticaTypeId, mock[JdbcLayerInterface])
    mockComplexTypeInfoResult(fieldTypeName, fieldId, typeId, rs, typeKind, typeName, precision, scale)
    (rs.next _).expects().returning(false)

    new ComplexTypesTable(jdbc).findComplexTypeInfo(verticaTypeId) match {
      case Left(err) => fail("Expected to succeed, Error: " + err.getFullContext)
      case Right(row) =>
        assert(row.fieldId == fieldId)
        assert(row.typeId == typeId)
        assert(row.fieldTypeName == fieldTypeName)
        assert(row.typeKind == typeKind)
        assert(row.typeName == typeName)
        assert(row.numericScale == scale)
        assert(row.numericPrecision == precision)
    }
  }

  it should "get types info" in {
    val verticaType = 100
    val typeName = "typeName"
    val jdbcType = 200
    val (jdbc, rs) = mockGetTypeInfo(verticaType, mock[JdbcLayerInterface])
    mockTypeInfoResult(verticaType, typeName, jdbcType, rs)
    (rs.next _).expects().returning(false)

    new TypesTable(jdbc).getVerticaTypeInfo(verticaType) match {
      case Left(_) => fail("Expected to succeed")
      case Right(typeInfo: TypeInfo) =>
        assert(typeInfo.typeName == typeName)
        assert(typeInfo.jdbcType == jdbcType)
        assert(typeInfo.typeId == verticaType)
    }
  }
}
