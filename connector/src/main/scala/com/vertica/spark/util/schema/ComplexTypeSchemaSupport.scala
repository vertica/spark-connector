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

import cats.data.NonEmptyList
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.{ConnectorError, ErrorList, QueryResultEmpty, UnsupportedVerticaType}
import com.vertica.spark.util.query.{ColumnsTable, ComplexTypeInfo, ComplexTypesTable, TypesTable}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructType}

import scala.annotation.tailrec

/**
 * Support class to read complex type schema from Vertica
 * */
object ComplexTypeSchemaSupport {
  //  This number is chosen from diff of array and set base id.
  val VERTICA_NATIVE_ARRAY_BASE_ID: Long = 1500L
  val VERTICA_SET_BASE_ID: Long = 2700L
  // This number is not defined be Vertica, so we use the delta of set and native array base id.
  val VERTICA_PRIMITIVES_MAX_ID:Long = VERTICA_SET_BASE_ID - VERTICA_NATIVE_ARRAY_BASE_ID
  val VERTICA_SET_MAX_ID: Long = VERTICA_SET_BASE_ID + VERTICA_PRIMITIVES_MAX_ID

  /**
   * Vertica's JDBC does not expose information needed to construct complex type structure of a column.
   * Thus, we have to query Vertica's system tables for this information.
   * Note that Vertica types has a different ID than JDBC types.
   * */
  def startQueryingVerticaComplexTypes(rootDef: ColumnDef, tableName: String, dbSchema: String, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    new ColumnsTable(jdbcLayer).getColumnInfo(rootDef.label, tableName, dbSchema) match {
      case Right(colInfo) =>
        queryVerticaTypes(rootDef.jdbcType, colInfo.verticaType, colInfo.precision, colInfo.scale, jdbcLayer)
          .map(columnDef => rootDef.copy(metadata = columnDef.metadata, children = columnDef.children))
      case Left(err) => Left(err)
    }
  }

  private def queryVerticaTypes(jdbcType: Int, verticaType: Long, precision: Long, scale: Long, jdbcLayer: JdbcLayerInterface) : ConnectorResult[ColumnDef] = {
    queryVerticaTypes2(verticaType, precision, scale, jdbcLayer)
    // jdbcType match {
    //   case java.sql.Types.ARRAY => queryVerticaArrayDef(verticaType, precision, scale, jdbcLayer)
    //   case java.sql.Types.STRUCT => queryVerticaRowDef(verticaType, jdbcLayer)
    //   case _ => queryVerticaPrimitiveDef(verticaType, precision, scale,jdbcLayer)
    // }
  }
  import cats.implicits._

  def typeKindToJdbc(typeKind: String): ConnectorResult[Int] = typeKind.toLowerCase match {
    case "row" => Right(java.sql.Types.STRUCT)
    case "array" => Right(java.sql.Types.ARRAY)
    case _ => Left(UnsupportedVerticaType(typeKind))
  }

  // Todo: implement Row support for reading.
  private def queryVerticaRowDef(verticaType: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    val complexTypesTable = new ComplexTypesTable(jdbcLayer)
    complexTypesTable.getComplexTypeFields(verticaType) match {
      case Left(value) => Left(value)
      case Right(results) => {
        val errorOrConnectorResults = results.map(field => {
          if (field.fieldTypeName.startsWith("_ct_")) {
            complexTypesTable.isArray(field.fieldId) match {
              case Left(error) => Left(error)
              case Right(isArray) => if(isArray) {
                queryVerticaTypes(java.sql.Types.ARRAY, field.fieldId, field.numericPrecision, field.numericScale, jdbcLayer)
              } else {
                queryVerticaTypes(java.sql.Types.STRUCT, field.fieldId, field.numericPrecision, field.numericScale, jdbcLayer)
              }
            }
          } else {
            queryVerticaTypes(-1, field.fieldId, field.numericPrecision, field.numericScale, jdbcLayer)
          }
        })

        listToEither(errorOrConnectorResults) match {
          case Left(error) => Left(error)
          case Right(fields) => Right(ColumnDef("", java.sql.Types.STRUCT, "Row", 0, 0, false, false, Metadata.empty, fields.toList))
        }
      }
    }
  }

  private def listToEither[T](list: Seq[Either[ConnectorError, T]]): Either[ErrorList, Seq[T]] = {
    list.toList
      // converts List[Either[A, B]] to Either[List[A], List[B]]
      .traverse(_.leftMap(err => NonEmptyList.one(err)).toValidated)
      .toEither
      .map(field => field)
      .left.map(errors => ErrorList(errors))
  }

  /**
   * Query Vertica system tables to find the array's depth and its element type.
   * */
  private def queryVerticaArrayDef(verticaTypeId: Long, precision: Long, scale: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    /**
     * A 1D primitive array is considered a native by Vertica and is tracked in types table.
     * Otherwise, nested arrays or arrays with complex elements are tracked in complex_types table.
     * We could tell a column type is a native array by looking at it's ID.
     * */
    // Native arrays id are smallest, started at 1500 + elementId
    val elementId = verticaTypeId - VERTICA_NATIVE_ARRAY_BASE_ID
    // Check if is Set, which starts at 2700 + elementId
    val isVerticaSet = elementId > VERTICA_PRIMITIVES_MAX_ID && elementId < VERTICA_SET_MAX_ID
    val elementVerticaId = if (isVerticaSet) verticaTypeId - VERTICA_SET_BASE_ID else elementId

    val isNative = elementVerticaId < VERTICA_PRIMITIVES_MAX_ID
    if (isNative) {
      queryVerticaNativeArray(elementVerticaId, isVerticaSet, precision, scale, jdbcLayer)
    } else {
      queryVerticaComplexArray(verticaTypeId ,jdbcLayer)
    }
  }

  private def queryVerticaNativeArray(elementVerticaTypeId: Long, isSet: Boolean, precision: Long, scale: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    val typesTable = new TypesTable(jdbcLayer)
    typesTable.getColumnDef(elementVerticaTypeId).map(result => {
      val elementDef: ColumnDef = makeArrayElementDef(result, 0)
      makeArrayDef("", isSet, 0, elementDef.copy(scale = scale.toInt, size = precision.toInt))
    })
  }

  private def makeArrayDef(colName: String, isSet: Boolean, depth: Long, elementDef: ColumnDef) = {
    val metadata = new MetadataBuilder()
      .putBoolean(MetadataKey.IS_VERTICA_SET, isSet)
      .putLong(MetadataKey.DEPTH, depth)
      .build()
    ColumnDef(colName, java.sql.Types.ARRAY, "Array", 0, 0, signed = false, nullable = true, metadata, List(elementDef))
  }

  private def makeArrayElementDef(baseDef: ColumnDef, depth: Long): ColumnDef = {
    val elementMetadata = new MetadataBuilder()
      .putLong(MetadataKey.DEPTH, depth)
      .build()
    baseDef.copy(label = "element", metadata = elementMetadata)
  }

  private def queryVerticaComplexArray(verticaTypeId: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    val complexTypeTable = new ComplexTypesTable(jdbcLayer)

    @tailrec
    def recursion(verticaType: Long, depth: Int): ConnectorResult[ColumnDef] = {
      // Traverse the complex_types table to find our element type.
      complexTypeTable.findComplexTypeInfo(verticaType) match {
        case Left(value) => Left(value)
        case Right(result: ComplexTypeInfo) =>
          if (result.fieldId < VERTICA_PRIMITIVES_MAX_ID) {
            queryVerticaTypes(-1, result.fieldId, result.numericPrecision, result.numericScale, jdbcLayer)
              .map(result => makeArrayDef("", isSet = false, depth, makeArrayElementDef(result, depth)))
          } else {
            complexTypeTable.isArray(result.fieldId) match {
              case Left(e) => ???
              case Right(isArray) => if(isArray){
                recursion(result.fieldId, depth + 1)
              } else {
                queryVerticaTypes(java.sql.Types.STRUCT, result.fieldId, result.numericPrecision, result.numericScale, jdbcLayer)
                  .map(result => makeArrayDef("", isSet = false, depth, makeArrayElementDef(result, depth)))
              }
            }
          }
      }
    }

    recursion(verticaTypeId, 0)
  }

  private def queryVerticaPrimitiveDef(verticaType: Long, precision: Long, scale: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    new TypesTable(jdbcLayer).getColumnDef(verticaType)
      .map(result => result.copy(size = precision.toInt, scale = scale.toInt))
  }

  private def queryVerticaTypes2(verticaTypeId: Long, precision: Long, scale: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    if(verticaTypeId < VERTICA_SET_MAX_ID){
      queryNativeType(verticaTypeId, precision, scale, jdbcLayer)
    } else {
      queryComplexType(verticaTypeId, precision, scale, jdbcLayer)
    }
  }

  def queryNativeType(verticaTypeId: Long, precision: Long, scale: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
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

        if (typeInfo.jdbcType == java.sql.Types.ARRAY) {
          val (isSet, elementId) = if (typeInfo.typeId > VERTICA_SET_BASE_ID) {
            (true, typeInfo.typeId - VERTICA_SET_BASE_ID)
          } else {
            (false, typeInfo.typeId - VERTICA_NATIVE_ARRAY_BASE_ID)
          }
          queryNativeType(elementId, precision, scale, jdbcLayer)
            .map(elementDef => {
              val metadata = new MetadataBuilder()
                .putBoolean(MetadataKey.IS_VERTICA_SET, isSet)
                .build()
              val element = elementDef.copy(metadata = new MetadataBuilder().putLong(MetadataKey.DEPTH, 0).build())
              ColumnDef("", jdbcType, typeName, size, _scale, isSigned, nullable ,metadata, List(element))
            })
        } else {
          Right(ColumnDef("", jdbcType, typeName, size, _scale, isSigned, nullable ,Metadata.empty))
        }
    }
  }

  def queryComplexType(verticaTypeId: Long, precision: Long, scale: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    val complexTypesTable = new ComplexTypesTable(jdbcLayer)

    @tailrec
    def getArrayElementDef(verticaTypeId: Long, precision: Long, scale: Long, depth: Long = 0): ConnectorResult[ColumnDef] = {
      complexTypesTable.getComplexTypeFields(verticaTypeId) match {
        case Left(value) => Left(value)
        case Right(fieldsInfo) =>
          fieldsInfo.headOption match {
            case None => Left(QueryResultEmpty(complexTypesTable.tableName, ""))
            case Some(typeInfo) =>
              if (typeInfo.typeKind.toLowerCase == "row") {
                val metadata = new MetadataBuilder().putLong(MetadataKey.DEPTH, depth).build()
                queryVerticaTypes2(typeInfo.typeId, precision, scale, jdbcLayer)
                  .map(_.copy(metadata = metadata))
              } else if (!typeInfo.fieldTypeName.startsWith("_ct_")) {
                val metadata = new MetadataBuilder().putLong(MetadataKey.DEPTH, depth).build()
                queryVerticaTypes2(typeInfo.fieldId, precision, scale, jdbcLayer)
                  .map(_.copy(metadata = metadata))
              } else {
                getArrayElementDef(typeInfo.fieldId, precision, scale, depth + 1)
              }
          }

      }
    }

    complexTypesTable.getComplexTypeFields(verticaTypeId) match {
      case Left(e) => Left(e)
      case Right(fields) =>
        val baseElement = fields.head
        baseElement.typeKind.toLowerCase match {
          case "array" => {
            val metadata = new MetadataBuilder().putBoolean(MetadataKey.IS_VERTICA_SET, false).build()
            getArrayElementDef(baseElement.fieldId, baseElement.numericPrecision, baseElement.numericScale)
              .map(element =>
                ColumnDef("", java.sql.Types.ARRAY, baseElement.typeKind, precision.toInt, scale.toInt, false, false, metadata, List(element)))
          }
          case "row" =>
            val errorsOrFields = fields.map(fieldInfo => queryVerticaTypes2(fieldInfo.fieldId, fieldInfo.numericPrecision, fieldInfo.numericScale, jdbcLayer))
            listToEither(errorsOrFields)
              .map(fields => ColumnDef("", java.sql.Types.STRUCT, baseElement.typeKind, 0, 0, false, false, Metadata.empty, fields.toList))
        }
    }
  }

}
