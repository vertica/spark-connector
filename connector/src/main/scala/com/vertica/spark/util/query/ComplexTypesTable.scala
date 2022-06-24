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
// scalastyle:off magic.number

/**
 * A row of complex_types table. Represents a component of the data structure type_id.
 * [[https://www.vertica.com/docs/latest/HTML/Content/Authoring/SQLReferenceManual/SystemTables/CATALOG/COMPLEX_TYPES.htm?zoom_highlight=complex%20type Documentations]]
 *
 * @param typeId The vertica type id of the complex structure.
 * @param fieldId the vertica type id of the field.
 *
 * */
case class ComplexTypeInfo(typeId: Long, typeName: String, fieldId: Long, fieldTypeName: String, numericScale: Long, typeKind: String, numericPrecision: Long, fieldName: String)

/**
 * When a complex type is created in Vertica, it's structure is recorded in this table.
 * Each row represents then a component (a field) of the complex structure, with the type_id being the vertica id of the complex type,
 * and field_id being the vertica id of the component. For example, a nested array will have as many rows as
 * its depth.
 * [[https://www.vertica.com/docs/latest/HTML/Content/Authoring/SQLReferenceManual/SystemTables/CATALOG/COMPLEX_TYPES.htm?zoom_highlight=complex%20type Documentations]]
 * */
class ComplexTypesTable(jdbcLayer: JdbcLayerInterface)
  extends VerticaTable[ComplexTypeInfo](jdbc = jdbcLayer) {

  override def tableName: String = "complex_types"

  override protected def columns: Seq[String] = List("type_id", "type_name", "field_id", "field_type_name", "numeric_scale", "type_kind", "numeric_precision", "field_name")

  override protected def buildRow(rs: ResultSet): ComplexTypeInfo = {
    // The column name should be in sync with the ones defined above.
    ComplexTypeInfo(
      rs.getLong("type_id"),
      rs.getString("type_name"),
      rs.getLong("field_id"),
      rs.getString("field_type_name"),
      rs.getLong("numeric_scale"),
      rs.getString("type_kind"),
      rs.getLong("numeric_precision"),
      rs.getString("field_name"))
  }

  def findComplexTypeInfo(verticaTypeId: Long): ConnectorResult[ComplexTypeInfo] = {
    val conditions = s"type_id=$verticaTypeId"
    super.selectWhereExpectOne(conditions)
  }

  def getComplexTypeFields(verticaTypeId: Long): ConnectorResult[Seq[ComplexTypeInfo]] = {
    val conditions = s"type_id=$verticaTypeId"
    super.selectWhere(conditions)
  }
}
