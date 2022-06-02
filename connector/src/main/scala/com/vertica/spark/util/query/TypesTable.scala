package com.vertica.spark.util.query

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.schema.ColumnDef
import org.apache.spark.sql.types.{DecimalType, Metadata}

import java.sql.ResultSet

case class TypeInfo(typeId: Long, jdbcType: Long, typeName: String, maxScale: Long)

class TypesTable(jdbcLayer: JdbcLayerInterface) extends VerticaTable[TypeInfo](jdbcLayer) {
  override protected def tableName: String = "types"

  override protected def columns: Seq[String] = List("type_id", "jdbc_type", "type_name", "max_scale")

  override protected def buildRow(rs: ResultSet): TypeInfo =
    TypeInfo(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getLong(4))

  def getVerticaTypeInfo(verticaType: Long): ConnectorResult[TypeInfo] = {
    val conditions = s"type_id=$verticaType"
    super.selectWhereExpectOne(conditions)
  }

  def getColumnDef(verticaType: Long): ConnectorResult[ColumnDef] = {
    getVerticaTypeInfo(verticaType)
      .map(typeInfo =>
        ColumnDef("", typeInfo.jdbcType.toInt, typeInfo.typeName, DecimalType.MAX_PRECISION, typeInfo.maxScale.toInt, true, false, Metadata.empty))
  }
}
