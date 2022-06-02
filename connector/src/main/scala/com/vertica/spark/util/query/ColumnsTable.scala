package com.vertica.spark.util.query

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.VerticaColumnNotFound

import java.sql.ResultSet

case class ColumnInfo(verticaType: Long, dataTypeName: String)

class ColumnsTable(jdbcLayer: JdbcLayerInterface) extends VerticaTable[ColumnInfo](jdbc = jdbcLayer) {

  override def tableName: String = "columns"

  override def columns: Seq[String] = List("data_type_id", "data_type", "data_type_length", "numeric_scale", "is_nullable")

  override def buildRow(resultSet: ResultSet): ColumnInfo = {
    ColumnInfo(
      resultSet.getLong(1),
      getTypeName(resultSet.getString(2)),
    )
  }

  /**
   * Type name report by Vertica could be INTEGER or ARRAY[...] or ROW(...)
   * and we want to extract just the type identifier
   * */
  def getTypeName(dataType:String) : String = {
    dataType
      .replaceFirst("\\[",",")
      .replaceFirst("\\(",",")
      .split(',')
      .head
  }

  def getColumnInfo(columnName: String, tableName: String, schema: String): ConnectorResult[ColumnInfo] = {
    val schemaCond = if(schema.nonEmpty) s" AND table_schema='$schema'" else ""
    val conditions = s"table_name='$tableName'$schemaCond AND column_name='$columnName'"
    super.selectWhereExpectOne(conditions)
  }

  def find(columnName: String, tableName: String): ConnectorResult[ColumnInfo] = {
    getColumnInfo(columnName, tableName, "")
  }
}