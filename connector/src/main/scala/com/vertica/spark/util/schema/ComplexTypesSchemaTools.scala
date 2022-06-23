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

package com.vertica.spark.util.schema

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.ConnectorResultUtils.listToEither
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.QueryResultEmpty
import com.vertica.spark.util.query.{ColumnsTable, ComplexTypeInfo, ComplexTypesTable, TypesTable}
import com.vertica.spark.util.schema.ComplexTypesSchemaTools.{VERTICA_NATIVE_ARRAY_BASE_ID, VERTICA_SET_BASE_ID, VERTICA_SET_MAX_ID}
import org.apache.spark.sql.types.{ArrayType, MapType, Metadata, MetadataBuilder, StructField, StructType}

import scala.annotation.tailrec

object ComplexTypesSchemaTools {
  //  This number is chosen from diff of array and set base id.
  val VERTICA_NATIVE_ARRAY_BASE_ID: Long = 1500L
  val VERTICA_SET_BASE_ID: Long = 2700L
  // This number is not defined be Vertica, so we use the delta of set and native array base id.
  val VERTICA_PRIMITIVES_MAX_ID:Long = VERTICA_SET_BASE_ID - VERTICA_NATIVE_ARRAY_BASE_ID
  val VERTICA_SET_MAX_ID: Long = VERTICA_SET_BASE_ID + VERTICA_PRIMITIVES_MAX_ID
}
/**
 * Support class to read complex type schema from Vertica
 * */
class ComplexTypesSchemaTools {

  /**
   * Vertica's JDBC does not expose information needed to construct complex type structure of a column.
   * Thus, we have to query Vertica's system tables for this information.
   * Note that Vertica types has a different ID than JDBC types
   * */
  def startQueryingVerticaComplexTypes(rootDef: ColumnDef, tableName: String, dbSchema: String, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    new ColumnsTable(jdbcLayer).getColumnInfo(rootDef.label, tableName, dbSchema) match {
      case Right(colInfo) => queryVerticaTypes(colInfo.verticaType, colInfo.precision, colInfo.scale, jdbcLayer)
          .map(columnDef => rootDef.copy(label = rootDef.label, metadata = columnDef.metadata, children = columnDef.children))
      case Left(err) => Left(err)
    }
  }

  private def queryVerticaTypes(verticaTypeId: Long, precision: Long, scale: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    if(verticaTypeId < VERTICA_SET_MAX_ID){
      queryNativeTypesTable(verticaTypeId, precision, scale, jdbcLayer)
    } else {
      queryComplexTypesTable(verticaTypeId, precision, scale, jdbcLayer)
    }
  }

  def queryNativeTypesTable(verticaTypeId: Long, precision: Long, scale: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    val typesTable = new TypesTable(jdbcLayer)
    typesTable.getVerticaTypeInfo(verticaTypeId) match {
      case Left(value) => Left(value)
      case Right(typeInfo) =>
        val jdbcType = typeInfo.jdbcType.toInt
        val typeName =  typeInfo.typeName
        val size = precision.toInt
        val _scale = scale.toInt
        val isSigned = typesTable.isSigned(jdbcType)
        val nullable = false

        def getNativeArray: ConnectorResult[ColumnDef] = {
          var (isSet, elementId) = if (typeInfo.typeId > VERTICA_SET_BASE_ID) {
            (true, typeInfo.typeId - VERTICA_SET_BASE_ID)
          } else {
            (false, typeInfo.typeId - VERTICA_NATIVE_ARRAY_BASE_ID)
          }

          //scalastyle:off
          // Special case. Array[Binary] has id of 1522, but Binary has id of 117
          elementId = if (elementId == 22) 117 else elementId
          //scalastyle:on

          queryNativeTypesTable(elementId, precision, scale, jdbcLayer)
            .map(elementDef => {
              val metadata = new MetadataBuilder()
                .putBoolean(MetadataKey.IS_VERTICA_SET, isSet)
                .build()
              val element = elementDef.copy(metadata = new MetadataBuilder().putLong(MetadataKey.DEPTH, 0).build())
              ColumnDef("", jdbcType, typeName, size, _scale, isSigned, nullable, metadata, List(element))
            })
        }

        if (typeInfo.jdbcType == java.sql.Types.ARRAY) {
          getNativeArray
        } else {
          Right(ColumnDef("", jdbcType, typeName, size, _scale, isSigned, nullable ,Metadata.empty))
        }
    }
  }

  private def queryComplexTypesTable(verticaTypeId: Long, precision: Long, scale: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    val complexTypesTable = new ComplexTypesTable(jdbcLayer)

    @tailrec
    def getArrayElementDef(componentInfo: ComplexTypeInfo, depth: Int): ConnectorResult[ColumnDef] = {
      if (componentInfo.typeKind.toLowerCase == "row") {
        val metadata = new MetadataBuilder().putLong(MetadataKey.DEPTH, depth - 1).build()
        queryVerticaTypes(componentInfo.typeId, componentInfo.numericPrecision, componentInfo.numericScale, jdbcLayer)
          .map(_.copy(metadata = metadata))
      } else if (componentInfo.typeKind.toLowerCase == "array" && componentInfo.fieldTypeName.startsWith("_ct_")) {
        complexTypesTable.getComplexTypeFields(componentInfo.fieldId) match {
          case Left(value) => Left(value)
          case Right(results) => results.headOption match {
            case None => Left(QueryResultEmpty(complexTypesTable.tableName, ""))
            case Some(head) => getArrayElementDef(head, depth + 1)
          }
        }
      } else {
        val metadata = new MetadataBuilder().putLong(MetadataKey.DEPTH, depth).build()
        queryVerticaTypes(componentInfo.fieldId, componentInfo.numericPrecision, componentInfo.numericScale, jdbcLayer)
          .map(_.copy(metadata = metadata))
      }
    }

    // Since each row in complex_types represents a component of the complex type,
    // First, query for the type and infer the root type.
    // Then, query for each components
    complexTypesTable.getComplexTypeFields(verticaTypeId) match {
      case Left(e) => Left(e)
      case Right(fields) =>
        val baseElement = fields.head
        baseElement.typeKind.toLowerCase match {
          case "array" =>
            val metadata = new MetadataBuilder().putBoolean(MetadataKey.IS_VERTICA_SET, false).build()
            getArrayElementDef(baseElement, 0).map(element =>
                ColumnDef("", java.sql.Types.ARRAY, baseElement.typeKind, precision.toInt, scale.toInt, false, false, metadata, List(element)))
          case "row" =>

            def getFieldDef(fieldInfo: ComplexTypeInfo) = {
              queryVerticaTypes(fieldInfo.fieldId, fieldInfo.numericPrecision, fieldInfo.numericScale, jdbcLayer)
                .map(colDef => colDef.copy(label = fieldInfo.fieldName))
            }

            val errorsOrRowFields = fields.map(getFieldDef)
            listToEither(errorsOrRowFields.toList)
              .map(fields => ColumnDef("", java.sql.Types.STRUCT, baseElement.typeKind, 0, 0, false, false, Metadata.empty, fields.toList))

          // case _ => Left()
        }
    }
  }

  def getComplexTypeColumns(schema: StructType):  List[StructField] = filterColumnTypes(schema)._2

  def getNativeTypeColumns(schema: StructType): List[StructField] = filterColumnTypes(schema)._1

  /**
   * @return A tuple of (nativeColumns, complexColumns)
   * */
  def filterColumnTypes(schema: StructType): (List[StructField], List[StructField]) = {
    val initialAccumulators: (List[StructField], List[StructField]) = (List(), List())
    schema.foldLeft(initialAccumulators)((acc, col) => {
      val (nativeCols, complexTypeCols) = acc
      if (isNativeType(col)) {
        (col :: nativeCols, complexTypeCols)
      } else {
        (nativeCols, col :: complexTypeCols)
      }
    })
  }

  /*
    * Check if field is a vertica native type. Vertica native types contains 1D arrays
    * */
  private def isNativeType(field: StructField): Boolean = {
    field.dataType match {
      case ArrayType(elementType, _) =>
        elementType match {
          case MapType(_, _, _) | StructType(_) | ArrayType(_, _) => false
          case _ => true
        }
      case MapType(_, _, _) | StructType(_) => false
      case _ => true
    }
  }
}
