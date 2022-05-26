package com.vertica.spark.util.query

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.VerticaColumnNotFound

import java.sql.ResultSet

case class ColumnInfo(dataTypeId: Long, dataType: String)

class ColumnsTable(jdbcLayer: JdbcLayerInterface) extends VerticaTable[ColumnInfo](jdbc = jdbcLayer) {

  override def tableName: String = "columns"

  override def columns: Seq[String] = List("data_type_id", "data_type")

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

  def getColumnType(columnName: String, tableName: String, schema: String): ConnectorResult[ColumnInfo] = {
    val schemaCond = if(schema.nonEmpty) s" AND table_schema='$schema'" else ""
    val conditions = s"table_name='$tableName'$schemaCond AND column_name='$columnName'"
    selectWhere(conditions) match {
      case Right(rows) =>
        if(rows.isEmpty)
          Left(VerticaColumnNotFound(columnName, tableName, schema))
        else
          Right(rows.head)
      case Left(err) => Left(err)
    }
  }

  def find(columnName: String, tableName: String): ConnectorResult[ColumnInfo] = {
    getColumnType(columnName, tableName, "")
  }
}