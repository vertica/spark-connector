package com.vertica.spark.util.query

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.query.VerticaTableTests.{mockComplexTypeInfoResult, mockGetColumnInfo, mockGetComplexTypeInfo, mockVerticaTableQuery}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.ResultSet

object VerticaTableTests extends VerticaTableTests {
  def wrapQuotation(str: String): String = "\"" + str +"\""

  def mockVerticaTableQuery(cols: Seq[String], tableName: String, where: String, jdbcLayer: JdbcLayerInterface): (JdbcLayerInterface, ResultSet) = {
    val rs = mock[ResultSet]
    val query = s"SELECT ${cols.map(wrapQuotation).mkString(", ")} FROM ${wrapQuotation(tableName)} WHERE $where"
    (jdbcLayer.query _).expects(query, *).returning(Right(rs))
    (rs.close _).expects
    (jdbcLayer, rs)
  }

  def mockGetColumnInfo(colName: String, tableName: String, schema: String, typeId: Long, typeName: String, jdbcLayer: JdbcLayerInterface): (JdbcLayerInterface, ResultSet) = {
    val schemaCond = if(schema.nonEmpty) s" AND table_schema='$schema'" else ""
    val conditions = s"table_name='$tableName'$schemaCond AND column_name='$colName'"
    val (jdbc, rs) = mockVerticaTableQuery(List("data_type_id" , "data_type"), "columns", conditions, jdbcLayer)

    (rs.next _).expects().returning(true)
    (rs.getLong: Int => Long).expects(1).returning(typeId)
    (rs.getString: Int => String).expects(2).returning(typeName)
    (rs.next _).expects().returning(false)

    (jdbc, rs)
  }

  def mockGetComplexTypeInfo(verticaTypeId: Long, jdbcLayer: JdbcLayerInterface): (JdbcLayerInterface, ResultSet) = {
    val conditions = s"type_id=$verticaTypeId"
    val (jdbc, rs) = mockVerticaTableQuery(List("field_type_name", "type_id", "field_id", "numeric_scale"), "complex_types", conditions, jdbcLayer)
    (rs.close _).expects
    (jdbc, rs)
  }

  def mockComplexTypeInfoResult(fieldTypeName:String, typeId: Long, fieldId: Long, rs: ResultSet): Unit = {
    (rs.next _).expects().returning(true)
    (rs.getString: Int => String).expects(1).returning(fieldTypeName)
    (rs.getLong: Int => Long).expects(2).returning(typeId)
    (rs.getLong: Int => Long).expects(3).returning(fieldId)
    val numericScale = ""
    (rs.getString: Int => String).expects(4).returning(numericScale)
  }

}

class VerticaTableTests extends AnyFlatSpec with MockFactory with org.scalatest.OneInstancePerTest {
  val colName = "col1"
  val tableName = "table1"
  val schemaName = "schema1"

  it should "find column info" in {
    val typeId = 1
    val typeName = "typeName"
    val (jdbcLayer, _) = mockGetColumnInfo(colName, tableName, "", typeId, typeName, mock[JdbcLayerInterface])

    new ColumnsTable(jdbcLayer)
      .getColumnType(colName, tableName, "") match {
      case Left(_) => fail("Expected to succeed")
      case Right(colInfo) =>
        assert(colInfo.dataType == typeName)
        assert(colInfo.dataTypeId == typeId)
    }
  }

  it should "find column info with schema" in {
    val typeId = 1
    val typeName = "typeName"
    val (jdbcLayer, _) = mockGetColumnInfo(colName, tableName, schemaName, typeId, typeName, mock[JdbcLayerInterface])

    new ColumnsTable(jdbcLayer)
      .getColumnType(colName, tableName, schemaName) match {
      case Left(_) => fail("Expected to succeed")
      case Right(colInfo) =>
        assert(colInfo.dataType == typeName)
        assert(colInfo.dataTypeId == typeId)
    }
  }

  it should "get complex types info" in {
    val verticaTypeId = 1
    val fieldTypeName = "fieldTypeName"
    val typeId = 1000000
    val fieldId = 2000000
    val (jdbc, rs) = mockGetComplexTypeInfo(verticaTypeId, mock[JdbcLayerInterface])
    mockComplexTypeInfoResult(fieldTypeName, typeId, fieldId, rs)
    (rs.next _).expects().returning(false)

    new ComplexTypesTable(jdbc).findComplexTypeInfo(verticaTypeId) match {
      case Left(err) => fail("Expected to succeed, Error: " + err.getFullContext)
      case Right(row) =>
        assert(row.fieldId == fieldId)
        assert(row.typeId == typeId)
        assert(row.fieldTypeName == fieldTypeName)
    }
  }
}
