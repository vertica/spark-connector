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

package com.vertica.spark.util.query

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult

import java.sql.ResultSet

case class ColumnInfo(verticaType: Long, dataTypeName: String, precision: Long, scale: Long)

// scalastyle:off magic.number
class ColumnsTable(jdbcLayer: JdbcLayerInterface) extends VerticaTable[ColumnInfo](jdbc = jdbcLayer) {

  override def tableName: String = "columns"

  override def columns: Seq[String] = List("data_type_id", "data_type", "numeric_precision", "numeric_scale")

  override def buildRow(resultSet: ResultSet): ColumnInfo = {
    ColumnInfo(
      resultSet.getLong(1),
      getTypeName(resultSet.getString(2)),
      resultSet.getLong(3),
      resultSet.getLong(4)
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
