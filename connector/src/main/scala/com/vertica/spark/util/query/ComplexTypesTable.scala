package com.vertica.spark.util.query

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.VerticaComplexTypeNotFound
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.schema.ColumnDef
import org.apache.spark.sql.types.Metadata

import java.sql.ResultSet

case class ComplexTypeInfo(typeId: Long, typeName: String, fieldId: Long, fieldTypeName: String, numericScale: Long, typeKind: String)

class ComplexTypesTable(jdbcLayer: JdbcLayerInterface) extends VerticaTable[ComplexTypeInfo](jdbc = jdbcLayer){

  override protected def tableName: String = "complex_types"

  override protected def columns: Seq[String] = List("type_id", "type_name", "field_id", "field_type_name", "numeric_scale", "type_kind")

  override protected def buildRow(rs: ResultSet): ComplexTypeInfo =
    ComplexTypeInfo(rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getString(4), rs.getLong(5), rs.getString(6))

  def findComplexTypeInfo(verticaTypeId: Long): ConnectorResult[ComplexTypeInfo] = {
    val conditions = s"type_id=$verticaTypeId"
    super.selectWhereExpectOne(conditions)
  }

  def getColumnDef(verticaTypeId: Long): ConnectorResult[ColumnDef] = {
    this.findComplexTypeInfo(verticaTypeId).map(ctInfo =>
      ColumnDef("", 0, ctInfo.fieldTypeName, 0, ctInfo.numericScale.toInt, true, false, Metadata.empty)
    )
  }
}
