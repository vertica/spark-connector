package com.vertica.spark.util.query

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.VerticaComplexTypeNotFound
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult

import java.sql.ResultSet

case class ComplexTypeInfo(fieldTypeName: String, typeId: Long, fieldId: Long, numericScale: String)

class ComplexTypesTable(jdbcLayer: JdbcLayerInterface) extends VerticaTable[ComplexTypeInfo](jdbc = jdbcLayer){

  override protected def tableName: String = "complex_types"

  override protected def columns: Seq[String] = List("field_type_name", "type_id", "field_id", "numeric_scale")

  override protected def buildRow(rs: ResultSet): ComplexTypeInfo =
    ComplexTypeInfo(rs.getString(1), rs.getLong(2), rs.getLong(3), rs.getString(4))

  def findComplexTypeInfo(verticaTypeId: Long): ConnectorResult[ComplexTypeInfo] = {
    val conditions = s"type_id=$verticaTypeId"
    selectWhere(conditions) match {
      case Left(error) => Left(error)
      case Right(rows) =>
        if(rows.isEmpty)
          Left(VerticaComplexTypeNotFound(verticaTypeId))
        else
          Right(rows.head)
    }
  }
}
