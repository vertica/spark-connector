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
import com.vertica.spark.util.schema.ColumnDef
import org.apache.spark.sql.types.{DecimalType, Metadata}

import java.sql.ResultSet

case class TypeInfo(typeId: Long, jdbcType: Long, typeName: String, maxScale: Long)

// scalastyle:off magic.number
/**
 * Vertica's types table contains type information of primitives and 1D array/set of primitive type.
 * */
class TypesTable(jdbcLayer: JdbcLayerInterface) extends VerticaTable[TypeInfo](jdbcLayer) {
  override protected def tableName: String = "types"

  override protected def columns: Seq[String] = List("type_id", "jdbc_type", "type_name", "max_scale")

  override protected def buildRow(rs: ResultSet): TypeInfo =
  // The column name should be in sync with the ones defined above.
    TypeInfo(
      rs.getLong("type_id"),
      rs.getLong("jdbc_type"),
      rs.getString("type_name"),
      rs.getLong("max_scale"))

  def getVerticaTypeInfo(verticaType: Long): ConnectorResult[TypeInfo] = {
    val conditions = s"type_id=$verticaType"
    super.selectWhereExpectOne(conditions)
  }

  private val signedList = List(
    java.sql.Types.DOUBLE,
    java.sql.Types.FLOAT,
    java.sql.Types.REAL,
    java.sql.Types.INTEGER,
    java.sql.Types.BIGINT,
    java.sql.Types.TINYINT,
    java.sql.Types.SMALLINT
  )

  def isSigned(jdbcType: Long): Boolean = signedList.contains(jdbcType)

  def getColumnDef(verticaType: Long): ConnectorResult[ColumnDef] = {
    getVerticaTypeInfo(verticaType)
      .map(typeInfo =>
        ColumnDef("",
          typeInfo.jdbcType.toInt,
          typeInfo.typeName,
          DecimalType.MAX_PRECISION,
          typeInfo.maxScale.toInt,
          signed = isSigned(typeInfo.jdbcType),
          nullable = false,
          Metadata.empty))
  }
}
