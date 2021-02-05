// (c) Copyright [2018-2021] Micro Focus or one of its affiliates.
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

import com.vertica.spark.datasource.jdbc._
import org.apache.spark.sql.types._
import java.sql.ResultSetMetaData

import cats.implicits._

import scala.util.Either
import cats.instances.list._
import com.vertica.spark.config.LogProvider
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.SchemaErrorType._

case class ColumnDef(
                      label: String,
                      colType: Int,
                      colTypeName: String,
                      size: Int,
                      scale: Int,
                      signed: Boolean,
                      nullable: Boolean,
                      metadata: Metadata)

trait SchemaToolsInterface {
  def readSchema(jdbcLayer: JdbcLayerInterface, tablename: String): Either[Seq[SchemaError], StructType]
  def getColumnInfo(jdbcLayer: JdbcLayerInterface, tablename: String) : Either[SchemaError, Seq[ColumnDef]]
}

class SchemaTools(val logProvider: LogProvider) extends SchemaToolsInterface {
  private val logger = logProvider.getLogger(classOf[SchemaTools])

  private def getCatalystType(
    sqlType: Int,
    precision: Int,
    scale: Int,
    signed: Boolean,
    typename: String): Either[SchemaError, DataType] = {
    println("The type is: " + sqlType)
    println("The type name is: " + typename)
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY => null
      case java.sql.Types.BIGINT =>  if (signed) { LongType } else { DecimalType(DecimalType.MAX_PRECISION,0)} //spark 2.x
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.BIT => BooleanType
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.DATALINK => null
      case java.sql.Types.DATE => DateType
      case java.sql.Types.DECIMAL => DecimalType(precision, scale)
      case java.sql.Types.DISTINCT => null
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.INTEGER => if (signed) { IntegerType } else { LongType }
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NCLOB => StringType
      case java.sql.Types.NULL => null
      case java.sql.Types.NUMERIC if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.NUMERIC => DecimalType(DecimalType.USER_DEFAULT.precision,DecimalType.USER_DEFAULT.scale) //spark 2.x
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER =>
        val typenameNormalized = typename.toLowerCase()
        if (typenameNormalized.startsWith("interval") || typenameNormalized.startsWith("uuid")) StringType else null
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.ROWID => LongType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType
      case java.sql.Types.STRUCT => StringType
      case java.sql.Types.TIME => StringType
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TINYINT => IntegerType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR => StringType
      case _ => null
    }

    if (answer == null) Left(SchemaError(MissingConversionError, sqlType.toString))
    else Right(answer)
  }

  def readSchema(jdbcLayer: JdbcLayerInterface, tablename: String) : Either[Seq[SchemaError], StructType] = {
    this.getColumnInfo(jdbcLayer, tablename) match {
      case Left(err) => Left(List(err))
      case Right(colInfo) =>
        val errorsOrFields: List[Either[SchemaError, StructField]] = colInfo.map(info => {
            this.getCatalystType(info.colType, info.size, info.scale, info.signed, info.colTypeName).map(columnType =>
              StructField(info.label, columnType, info.nullable, info.metadata))
          }).toList
        errorsOrFields
          // converts List[Either[A, B]] to Either[List[A], List[B]]
          .traverse(_.leftMap(err => List(err)).toValidated).toEither
          .map(field => StructType(field))
    }
  }

  def getColumnInfo(jdbcLayer: JdbcLayerInterface, tablename: String) : Either[SchemaError, Seq[ColumnDef]] = {
    // Query for an empty result set from Vertica.
    // This is simply so we can load the metadata of the result set
    // and use this to retrieve the name and type information of each column
    jdbcLayer.query("SELECT * FROM " + tablename + " WHERE 1=0") match {
      case Left(err) => Left(SchemaError(JdbcError, err.msg))
      case Right(rs) =>
        try {
          val rsmd = rs.getMetaData
          Right((1 to rsmd.getColumnCount).map(idx => {
            val columnLabel = rsmd.getColumnLabel(idx)
            val dataType = rsmd.getColumnType(idx)
            val typeName = rsmd.getColumnTypeName(idx)
            val fieldSize = DecimalType.MAX_PRECISION
            val fieldScale = rsmd.getScale(idx)
            val isSigned = rsmd.isSigned(idx)
            val nullable = rsmd.isNullable(idx) != ResultSetMetaData.columnNoNulls
            val metadata = new MetadataBuilder().putString("name", columnLabel).build()
            ColumnDef(columnLabel, dataType, typeName, fieldSize, fieldScale, isSigned, nullable, metadata)
          }))
        }
        catch {
          case e: Throwable =>
            logger.error("Could not get column info: ", e)
            Left(SchemaError(UnexpectedExceptionError, e.getMessage))
        }
        finally {
          rs.close()
        }
    }
  }
}

