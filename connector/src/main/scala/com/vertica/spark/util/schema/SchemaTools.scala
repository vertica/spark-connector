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
import cats.implicits._
import com.vertica.spark.config._
import com.vertica.spark.datasource.jdbc._
import com.vertica.spark.util.complex.ComplexTypeUtils
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ErrorHandling.{listToEitherSchema, ConnectorResult, SchemaResult}
import com.vertica.spark.util.query.{ColumnInfo, ColumnsTable, ComplexTypesTable, StringParsingUtils}
import com.vertica.spark.util.schema.ComplexTypesSchemaTools.{VERTICA_NATIVE_ARRAY_BASE_ID, VERTICA_SET_MAX_ID}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.types._

import java.sql.ResultSetMetaData
import scala.annotation.tailrec
import scala.util.{Either, Try}
import scala.util.control.Breaks.{break, breakable}

/**
 * Object for holding column information.
 * */
case class ColumnDef(
                      label: String,
                      jdbcType: Int,
                      colTypeName: String,
                      size: Int,
                      scale: Int,
                      signed: Boolean,
                      nullable: Boolean,
                      metadata: Metadata = Metadata.empty,
                      children: List[ColumnDef] = Nil
                    )

object MetadataKey {
  val NAME = "name"
  val IS_VERTICA_SET = "is_vertica_set"
  val DEPTH = "depth"
}

/**
 * Interface for functionality around retrieving and translating SQL schema between Spark and Vertica.
 */
trait SchemaToolsInterface {
  /**
   * Retrieves the schema of Vertica table in Spark format.
   *
   * @param jdbcLayer Depedency for communicating with Vertica over JDBC
   * @param tableSource The table/query we want the schema of
   * @return StructType representing table's schema converted to Spark's schema type.
   */
  def readSchema(jdbcLayer: JdbcLayerInterface, tableSource: TableSource): ConnectorResult[StructType]

  /**
   * Retrieves the schema of Vertica table in format of list of column definitions.
   *
   * @param jdbcLayer Dependency for communicating with Vertica over JDBC
   * @param tableSource The table/query we want the schema of.
   * @return Sequence of ColumnDef, representing the Vertica structure of schema.
   */
  def getColumnInfo(jdbcLayer: JdbcLayerInterface, tableSource: TableSource): ConnectorResult[Seq[ColumnDef]]

  /**
   * Returns the Vertica type to use for a given Spark type.
   *
   * @param sparkType One of Sparks' DataTypes
   * @param strlen Necessary if the type is StringType, string length to use for Vertica type.
   * @param arrayLength Necessary if the type is ArrayType, array element length to use for Vertica type.
   * @param metadata metadata of struct field. Necessary to infer Vertica array vs Vertica Set.
   * @return String representing Vertica type, that one could use in a create table statement
   */
  def getVerticaTypeFromSparkType (sparkType: org.apache.spark.sql.types.DataType, strlen: Long, arrayLength: Long, metadata: Metadata): SchemaResult[String]

  /**
   * Compares table schema and spark schema to return a list of columns to use when copying spark data to the given Vertica table.
   *
   * @param jdbcLayer Depedency for communicating with Vertica over JDBC
   * @param tableName Name of the table we want to copy to.
   * @param schema Schema of data in spark.
   * @return
   */
  def getCopyColumnList(jdbcLayer: JdbcLayerInterface, tableName: TableName, schema: StructType): ConnectorResult[String]

  /**
   * Matches a list of columns against a required schema, only returning the list of matches in string form.
   *
   * @param columnDefs List of column definitions from the Vertica table.
   * @param requiredSchema Set of columns in Spark schema format that we want to limit the column list to.
   * @return List of columns in matches.
   */
  def makeColumnsString(columnDefs: Seq[ColumnDef], requiredSchema: StructType): String

  /**
   * Converts spark schema to table column defs in Vertica format for use in a CREATE TABLE statement
   *
   * @param schema Schema in spark format
   * @return List of column names and types, that can be used in a Vertica CREATE TABLE.
   * */
  def makeTableColumnDefs(schema: StructType, strlen: Long, arrayLength: Long): ConnectorResult[String]

  /**
   * Gets a list of column values to be inserted within a merge.
   *
   * @param copyColumnList String of columns passed in by user as a configuration option.
   * @return String of values to append to INSERT VALUES in merge.
   */
  def getMergeInsertValues(jdbcLayer: JdbcLayerInterface, tableName: TableName, copyColumnList: Option[ValidColumnList]): ConnectorResult[String]

  /**
   * Gets a list of column values and their updates to be updated within a merge.
   *
   * @param copyColumnList String of columns passed in by user as a configuration option.
   * @param tempTableName Temporary table created as part of merge statement
   * @return String of columns and values to append to UPDATE SET in merge.
   */
  def getMergeUpdateValues(jdbcLayer: JdbcLayerInterface, tableName: TableName, tempTableName: TableName, copyColumnList: Option[ValidColumnList]): ConnectorResult[String]

  /**
  * Replaces columns of unknown type with partial schema specified with empty DF
  *
  * @param createExternalTableStmt Original create table statement retrieved using SELECT INFER_EXTERNAL_TABLE_DDL
  * @param schema Schema passed in with empty dataframe
  * @return Updated create external table statement
  */
  def inferExternalTableSchema(createExternalTableStmt: String, schema: StructType, tableName: String, strlen: Long, arrayLength: Long): ConnectorResult[String]

  /**
   * Check if the schema is valid for as an internal Vertica table.
   *
   * @param schema schema of the table
   */
  def checkValidTableSchema(schema: StructType): ConnectorResult[Unit]

  /**
   * Given a query, append the schema to the FROM clause table.
   * Ignore if FROM clause is a sub query or already has a schema.
   *
   * @param query an SQL query
   * @param schema an optional schema to be added.
   * @return the query with the added schema.
   * */
  def addDbSchemaToQuery(query: String, schema: Option[String]): String
}

class SchemaTools(ctTools: ComplexTypesSchemaTools = new ComplexTypesSchemaTools) extends SchemaToolsInterface {
  private val logger = LogProvider.getLogger(classOf[SchemaTools])
  private val unknown = "UNKNOWN"
  private val maxlength = "maxlength"
  private val longlength = 65000
  private val complexTypeUtils = new ComplexTypeUtils()

  private def addDoubleQuotes(str: String): String = {
    "\"" + str + "\""
  }

  private def getCatalystType(
                               sqlType: Int,
                               precision: Int,
                               scale: Int,
                               signed: Boolean,
                               typename: String,
                               childDefs: List[ColumnDef]): Either[SchemaError, DataType] = {
    sqlType match {
      case java.sql.Types.ARRAY => getArrayType(childDefs)
      case java.sql.Types.STRUCT => getStructType(childDefs)
      case _ => getCatalystTypeFromJdbcType(sqlType, precision, scale, signed, typename)
    }
  }

  private def getStructType(fields: List[ColumnDef]): Either[SchemaError, DataType] = {
    val fieldDefs = fields.map(colDef => {
        getCatalystType(colDef.jdbcType, colDef.size, colDef.scale, colDef.signed, colDef.colTypeName, colDef.children)
          .map(dataType => StructField(colDef.label, dataType, colDef.nullable, colDef.metadata))
      })
    listToEitherSchema(fieldDefs)
      .map(fields => StructType(fields))
  }

  private def getArrayType(elementDef: List[ColumnDef]): Either[SchemaError, ArrayType] = {
    @tailrec
    def makeNestedArrays(arrayDepth: Long, arrayElement: DataType): ArrayType = {
      if(arrayDepth > 0) {
        makeNestedArrays(arrayDepth - 1, ArrayType(arrayElement))
      } else {
        ArrayType(arrayElement)
      }
    }

    elementDef.headOption match {
      case Some(element) =>
        getCatalystType(element.jdbcType, element.size, element.scale, element.signed, element.colTypeName, element.children)
          .map(elementType => makeNestedArrays(element.metadata.getLong(MetadataKey.DEPTH), elementType))
      case None => Left(MissingElementTypeError())
    }
  }

  protected def getCatalystTypeFromJdbcType(sqlType: Int,
                                          precision: Int,
                                          scale: Int,
                                          signed: Boolean,
                                          typename: String): Either[MissingSqlConversionError, DataType] = {

    // scalastyle:off
    val answer = sqlType match {
      case java.sql.Types.BIGINT => if (signed) { LongType } else { DecimalType(DecimalType.MAX_PRECISION, 0) } //spark 2.x
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
      case java.sql.Types.NUMERIC => DecimalType(DecimalType.USER_DEFAULT.precision, DecimalType.USER_DEFAULT.scale) //spark 2.x
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER =>
        val typenameNormalized = typename.toLowerCase()
        if (typenameNormalized.startsWith("interval") || typenameNormalized.startsWith("uuid")) StringType else null
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.ROWID => LongType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType
      case java.sql.Types.TIME => StringType
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TINYINT => IntegerType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR => StringType
      case _ => null
    }

    if (answer == null) Left(MissingSqlConversionError(sqlType.toString, typename))
    else Right(answer)
  }

  def readSchema(jdbcLayer: JdbcLayerInterface, tableSource: TableSource): ConnectorResult[StructType] = {
    this.getColumnInfo(jdbcLayer, tableSource) match {
      case Left(err) => Left(err)
      case Right(colInfo) =>
        val errorsOrFields: List[Either[SchemaError, StructField]] = colInfo.map(info => {
          this.getCatalystType(info.jdbcType, info.size, info.scale, info.signed, info.colTypeName, info.children)
            .map(columnType => StructField(info.label, columnType, info.nullable, info.metadata))
        }).toList
        errorsOrFields
          // converts List[Either[A, B]] to Either[List[A], List[B]]
          .traverse(_.leftMap(err => NonEmptyList.one(err)).toValidated).toEither
          .map(field => StructType(field))
          .left.map(errors => ErrorList(errors))
    }
  }

  def getColumnInfo(jdbcLayer: JdbcLayerInterface, tableSource: TableSource): ConnectorResult[Seq[ColumnDef]] = {
    val emptyQuery = tableSource match {
      case tb: TableName => "SELECT * FROM " + tb.getFullTableName + " WHERE 1=0"
      case TableQuery(query, _, schema) => "SELECT * FROM (" + addDbSchemaToQuery(query, schema) + ") AS x WHERE 1=0"
    }
   // We query an empty result to get the table's metadata.
    jdbcLayer.query(emptyQuery) match {
      case Left(err) => Left(JdbcSchemaError(err))
      case Right(rs) =>
        try {
          val rsmd = rs.getMetaData
          val colDefsOrErrors: List[ConnectorResult[ColumnDef]] =
            (1 to rsmd.getColumnCount)
              .map(idx => {
                val columnLabel = rsmd.getColumnLabel(idx)
                val typeName = rsmd.getColumnTypeName(idx)
                val fieldSize = DecimalType.MAX_PRECISION
                val fieldScale = rsmd.getScale(idx)
                val isSigned = rsmd.isSigned(idx)
                val nullable = rsmd.isNullable(idx) != ResultSetMetaData.columnNoNulls
                val metadata = new MetadataBuilder().putString(MetadataKey.NAME, columnLabel).build()
                val colType = rsmd.getColumnType(idx)
                val colDef = ColumnDef(columnLabel, colType, typeName, fieldSize, fieldScale, isSigned, nullable, metadata)
                tableSource match {
                  case tb: TableName =>
                    val unQuotedName = tb.getTableName.replaceAll("\"", "")
                    val unQuotedDbSchema = tb.getDbSchema.replaceAll("\"", "")
                    colType match {
                      case java.sql.Types.ARRAY | java.sql.Types.STRUCT =>
                        ctTools.startQueryingVerticaComplexTypes(colDef, unQuotedName, unQuotedDbSchema, jdbcLayer)
                      case _ => Right(colDef)
                    }
                  case query: TableQuery =>
                    colType match {
                      case java.sql.Types.ARRAY | java.sql.Types.STRUCT => Left(QueryReturnsComplexTypes(columnLabel, typeName, query.query))
                      case _ => Right(colDef)
                    }
                }
              }).toList
          colDefsOrErrors
            .traverse(_.leftMap(err => NonEmptyList.one(err)).toValidated).toEither
            .map(columnDef => columnDef)
            .left.map(errors => ErrorList(errors))
        }
        catch {
          case e: Throwable =>
            Left(DatabaseReadError(e).context("Could not get column info from Vertica"))
        }
        finally {
          rs.close()
        }
    }
  }

  override def getVerticaTypeFromSparkType(sparkType: DataType, strlen: Long, arrayLength: Long, metadata: Metadata): SchemaResult[String] = {
    sparkType match {
      case org.apache.spark.sql.types.MapType(keyType, valueType, _) => sparkMapToVerticaMap(keyType, valueType, strlen)
      case org.apache.spark.sql.types.StructType(fields) => sparkStructToVerticaRow(fields, strlen, arrayLength)
      case org.apache.spark.sql.types.ArrayType(sparkType,_) => sparkArrayToVerticaArray(sparkType, strlen, arrayLength, metadata)
      case _ => this.sparkPrimitiveToVerticaPrimitive(sparkType, strlen)
    }
  }

  private def sparkMapToVerticaMap(keyType: DataType, valueType: DataType, strlen: Long): SchemaResult[String] = {
    val keyVerticaType = this.sparkPrimitiveToVerticaPrimitive(keyType, strlen)
    val valueVerticaType = this.sparkPrimitiveToVerticaPrimitive(valueType, strlen)
    if (keyVerticaType.isRight && valueVerticaType.isRight)
      Right(s"MAP<${keyVerticaType.right.get}, ${valueVerticaType.right.get}>")
    else {
      val keyErrorMsg = keyVerticaType match {
        case Left(error) => error.getFullContext
        case Right(_) => "None"
      }
      val valueErrorMsg = valueVerticaType match {
        case Left(error) => error.getFullContext
        case Right(_) => "None"
      }
      Left(MapDataTypeConversionError(keyErrorMsg, valueErrorMsg))
    }
  }

  private def sparkStructToVerticaRow(fields: Array[StructField], strlen: Long, arrayLength: Long): SchemaResult[String] = {
    makeTableColumnDefs(StructType(fields), strlen, arrayLength) match {
      case Left(err) => Left(StructFieldsError(err))
      case Right(fieldDefs) =>
        Right("ROW" +
          fieldDefs
            // Row definition cannot have constraints
            .replace(" NOT NULL", "")
            .trim())
    }
  }

  private def sparkArrayToVerticaArray(dataType: DataType, strlen: Long, arrayLength: Long, metadata: Metadata): SchemaResult[String] = {
    val length = if (arrayLength <= 0) "" else s",$arrayLength"
    val isSet = Try{metadata.getBoolean(MetadataKey.IS_VERTICA_SET)}.getOrElse(false)
    val keyword = if(isSet) "SET" else "ARRAY"

    @tailrec
    def recursion(dataType: DataType, leftAccumulator: String, rightAccumulator: String, depth: Int): SchemaResult[String] = {
      dataType match {
        case ArrayType(elementType, _) =>
          recursion(elementType, s"$leftAccumulator$keyword[", s"$length]$rightAccumulator", depth + 1)
        case _ =>
          this.getVerticaTypeFromSparkType(dataType, strlen, arrayLength, metadata) match {
            case Right(verticaType) =>
              Right(s"$leftAccumulator$verticaType$rightAccumulator")
            case Left(error) => Left(error)
          }
      }
    }

    recursion(dataType, s"$keyword[", s"$length]", 0)
  }

  private def sparkPrimitiveToVerticaPrimitive(sparkType: org.apache.spark.sql.types.DataType, strlen: Long): SchemaResult[String] = {
    sparkType match {
      case org.apache.spark.sql.types.BinaryType => Right("VARBINARY(" + longlength + ")")
      case org.apache.spark.sql.types.BooleanType => Right("BOOLEAN")
      case org.apache.spark.sql.types.ByteType => Right("TINYINT")
      case org.apache.spark.sql.types.DateType => Right("DATE")
      case org.apache.spark.sql.types.CalendarIntervalType => Right("INTERVAL")
      case decimalType: org.apache.spark.sql.types.DecimalType =>
        if(decimalType.precision == 0)
          Right("DECIMAL")
        else
          Right(s"DECIMAL(${decimalType.precision}, ${decimalType.scale})")
      case org.apache.spark.sql.types.DoubleType => Right("DOUBLE PRECISION")
      case org.apache.spark.sql.types.FloatType => Right("FLOAT")
      case org.apache.spark.sql.types.IntegerType => Right("INTEGER")
      case org.apache.spark.sql.types.LongType => Right("BIGINT")
      case org.apache.spark.sql.types.NullType => Right("null")
      case org.apache.spark.sql.types.ShortType => Right("SMALLINT")
      case org.apache.spark.sql.types.TimestampType => Right("TIMESTAMP")
      case org.apache.spark.sql.types.StringType =>
        // here we constrain to 32M, max long type size
        // and default to VARCHAR for sizes <= 65K
        val vtype = if (strlen > longlength) "LONG VARCHAR" else "VARCHAR"
        Right(vtype + "(" + strlen.toString + ")")
      case _ => Left(MissingSparkPrimitivesConversionError(sparkType))
    }
  }

  def getCopyColumnList(jdbcLayer: JdbcLayerInterface, tableName: TableName, schema: StructType): ConnectorResult[String] = {
    for {
      columns <- getColumnInfo(jdbcLayer, tableName)

      columnList <- {
        val colCount = columns.length
        var colsFound = 0
        columns.foreach (column => {
          logger.debug("Will check that target column: " + column.label + " exist in DF")
          breakable {
            schema.foreach(s => {
              logger.debug("Comparing target table column: " + column.label + " with DF column: " + s.name)
              if (s.name.equalsIgnoreCase(column.label)) {
                colsFound += 1
                logger.debug("Column: " + s.name + " found in target table and DF")
                // Data types compatibility is already verified by COPY
                // Check nullability
                // Log a warning if target column is not null and DF column is null
                if (!column.nullable) {
                  if (s.nullable) {
                    logger.warn("S2V: Column " + s.name + " is NOT NULL in target table " + tableName.getFullTableName +
                      " but it's nullable in the DataFrame. Rows with NULL values in column " +
                      s.name + " will be rejected.")
                  }
                }
                break
              }
            })
          }
        })
        // Verify DataFrame column count <= target table column count
        if (!(schema.length <= colCount)) {
          Left(TableNotEnoughRowsError().context("Error: Number of columns in the target table should be greater or equal to number of columns in the DataFrame. "
            + " Number of columns in DataFrame: " + schema.length + ". Number of columns in the target table: "
            + tableName.getFullTableName + ": " + colCount))
        }
        // Load by Name:
        // if all cols in DataFrame were found in target table
        else if (colsFound == schema.length) {
          var columnList = ""
          var first = true
          schema.foreach(s => {
            if (first) {
              columnList = "\"" + s.name
              first = false
            }
            else {
              columnList += "\",\"" + s.name
            }
          })
          columnList = "(" + columnList + "\")"
          logger.info("Load by name. Column list: " + columnList)
          Right(columnList)
        }

        else {
          // Load by position:
          // If not all column names in the schema match column names in the target table
          logger.info("Load by Position")
          Right("")
        }
      }
    } yield columnList
  }

  def makeColumnsString(columnDefs: Seq[ColumnDef], requiredSchema: StructType): String = {
    val requiredColumnDefs: Seq[ColumnDef] = if (requiredSchema.nonEmpty) {
      columnDefs.filter(cd => requiredSchema.fields.exists(field => field.name == cd.label))
    } else {
      columnDefs
    }

    def castToVarchar: String => String = colName => colName + "::varchar AS " + addDoubleQuotes(colName)

    def castToArray(colInfo: ColumnDef): String = {
      val colName = addDoubleQuotes(colInfo.label)
      colInfo.children.headOption match {
        case Some(element) => s"($colName::ARRAY[${element.colTypeName}]) as $colName"
        case None => s"($colName::ARRAY[UNKNOWN]) as $colName"
      }
    }

    requiredColumnDefs.map(info => {
      val colLabel = addDoubleQuotes(info.label)
      info.jdbcType match {
        case java.sql.Types.OTHER =>
          val typenameNormalized = info.colTypeName.toLowerCase()
          if (typenameNormalized.startsWith("interval") ||
            typenameNormalized.startsWith("uuid")) {
            castToVarchar(info.label)
          } else {
            colLabel
          }
        case java.sql.Types.TIME => castToVarchar(info.label)
        case java.sql.Types.ARRAY =>
          val isSet = Try{info.metadata.getBoolean(MetadataKey.IS_VERTICA_SET)}.getOrElse(false)
          // Casting on Vertica side as a work around until Vertica Export supports Set
          if(isSet) castToArray(info) else colLabel
        case _ => colLabel
      }
    }).mkString(",")
  }

  def makeTableColumnDefs(schema: StructType, strlen: Long, arrayLength: Long): ConnectorResult[String] = {
    val colDefsOrErrors = schema.map(col => {

      val colName = if(col.name.isEmpty){
        ""
      } else {
        "\"" + col.name + "\""
      }

      val notNull = if (!col.nullable) "NOT NULL" else ""
      getVerticaTypeFromSparkType(col.dataType, strlen, arrayLength, col.metadata) match {
        case Left(err) => Left(SchemaConversionError(err).context("Schema error when trying to create table"))
        case Right(colType) =>
          Right(s"$colName $colType $notNull".trim())
      }
    }).toList

    val result = colDefsOrErrors
      // converts List[Either[A, B]] to Either[List[A], List[B]]
      .traverse(_.leftMap(err => NonEmptyList.one(err)).toValidated).toEither
      .map(columnDef => columnDef)
      .left.map(errors => ErrorList(errors))

    result match {
      case Right(colDefList) => Right(s" (${colDefList.mkString(", ")})")
      case Left(err) => Left(err)
    }
  }

  def getMergeInsertValues(jdbcLayer: JdbcLayerInterface, tableName: TableName, copyColumnList: Option[ValidColumnList]): ConnectorResult[String] = {
    val valueList = getColumnInfo(jdbcLayer, tableName) match {
      case Right(info) => Right(info.map(x => "temp." + addDoubleQuotes(x.label)).mkString(","))
      case Left(err) => Left(JdbcSchemaError(err))
    }
    valueList
  }

  def checkValidTableSchema(schema: StructType): ConnectorResult[Unit] = {
    for {
      _ <- checkBlankColumnNames(schema)
      _ <- checkComplexTypesSchema(schema)
    } yield()
  }

  /**
   * Complex type columns needs at least one native type column in the schema
   * */
  private def checkComplexTypesSchema(schema: StructType): ConnectorResult[Unit] = {
    val (nativeCols, complexTypeCols) = complexTypeUtils.getComplexTypeColumns(schema)
    if (nativeCols.isEmpty) {
      if (complexTypeCols.nonEmpty)
        Left(InvalidTableSchemaComplexType())
      else
        Left(EmptySchemaError())
    } else {
      checkMapColumnsSchema(complexTypeCols)
    }
  }

  private def checkMapColumnsSchema(complexTypeCols: List[StructField]) = {
    complexTypeCols.filter(_.dataType.isInstanceOf[MapType])
      .map(col => checkMapContainsPrimitives(col.name, col.dataType.asInstanceOf[MapType]))
      // converts List[Either[A, B]] to Either[List[A], List[B]]
      .traverse(_.leftMap(err => NonEmptyList.one(err)).toValidated).toEither
      .map(_ => {})
      .left.map(errors => ErrorList(errors))
  }

  private def checkMapContainsPrimitives(colName: String, map: MapType): ConnectorResult[Unit] = {
    val keyType = this.sparkPrimitiveToVerticaPrimitive(map.keyType, 0)
    val valueType = this.sparkPrimitiveToVerticaPrimitive(map.valueType, 0)
    if(keyType.isRight && valueType.isRight) Right()
    else Left(InvalidMapSchemaError(colName))
  }

  def getMergeUpdateValues(jdbcLayer: JdbcLayerInterface, tableName: TableName, tempTableName: TableName, copyColumnList: Option[ValidColumnList]): ConnectorResult[String] = {
    val columnList = copyColumnList match {
      case Some(list) => {
        val customColList = list.toString.split(",").toList.map(col => col.trim())
        val colList = getColumnInfo(jdbcLayer, tempTableName) match {
          case Right(info) =>
            val tupleList = customColList zip info
            Right(tupleList.map(x => addDoubleQuotes(x._1) + "=temp." + addDoubleQuotes(x._2.label)).mkString(", "))
          case Left(err) => Left(JdbcSchemaError(err))
        }
        colList
      }
      case None => {
        val updateList = getColumnInfo(jdbcLayer, tempTableName) match {
          case Right(info) => Right(info.map(x => addDoubleQuotes(x.label) + "=temp." + addDoubleQuotes(x.label)).mkString(", "))
          case Left(err) => Left(JdbcSchemaError(err))
        }
        updateList
      }
    }
    columnList
  }

  def updateFieldDataType(col: String, colName: String, schema: StructType, strlen: Long, arrayLength: Long): String = {
    val fieldType = schema.collect {
      case field: StructField if (addDoubleQuotes(field.name) == colName) =>
         if (field.metadata.contains(maxlength) && field.dataType.simpleString == "string") {
           if (field.metadata.getLong(maxlength) > longlength) {
             "long varchar(" + field.metadata.getLong(maxlength).toString + ")"
           } else{
             "varchar(" + field.metadata.getLong(maxlength).toString + ")"
           }
         }
         else if (field.metadata.contains(maxlength) && field.dataType.simpleString == "binary") {
           "varbinary(" + field.metadata.getLong(maxlength).toString + ")"
         }
         else {
           getVerticaTypeFromSparkType(field.dataType, strlen, arrayLength, field.metadata) match {
             case Right(dataType) => dataType
             case Left(err) => Left(err)
           }
         }
    }

    if(fieldType.nonEmpty) {
      colName + " " + fieldType.head
    } else {
      col
    }
  }

  def inferExternalTableSchema(createExternalTableStmt: String, schema: StructType, tableName: String, strlen: Long, arrayLength: Long): ConnectorResult[String] = {
    val stmt = createExternalTableStmt.replace("\"" + tableName + "\"", tableName)
    val (openParen, closingParen) = StringParsingUtils.findFirstParenGroupIndices(stmt)
    val schemaString = stmt.substring(openParen + 1, closingParen)
    val schemaList = StringParsingUtils.splitByComma(schemaString)
    val updatedSchema: String = schemaList.map(colDef => {
      val colName = colDef.trim.split(" ").head

      if(schema.nonEmpty){
        updateFieldDataType(colDef, colName, schema, strlen, arrayLength)
      }
      else if(colDef.toLowerCase.contains("varchar")) colName + " varchar(" + strlen + ")"
      else if(colDef.toLowerCase.contains("varbinary")) colName + " varbinary(" + longlength + ")"
      else colDef
    }).mkString(",")

    if(updatedSchema.contains(unknown)) {
      Left(UnknownColumnTypesError().context(unknown + " partitioned column data type."))
    }
    else {
      val updatedCreateTableStmt = stmt.replace(schemaString, updatedSchema)
      logger.info("Updated create external table statement: " + updatedCreateTableStmt)
      Right(updatedCreateTableStmt)
    }
  }

  def checkBlankColumnNames(schema: StructType): ConnectorResult[Unit] = {

    @tailrec
    def findEmptyColumnName(fields: List[StructField]): ConnectorResult[Unit] = {
      fields.headOption match {
        case Some(column) =>
          if (StringUtils.isBlank(column.name)) {
            Left(BlankColumnNamesError())
          } else {
            findEmptyColumnName(fields.tail)
          }
        case None => Right()
      }
    }

    // Use recursion to break early
    findEmptyColumnName(schema.fields.toList)
  }

  def addDbSchemaToQuery(query: String, dbSchema: Option[String]): String = {

    // The datasource syntax uses dots to separate schema and database name of the table.
    // So finding a dot means we do not add a schema
    // Docs: https://www.vertica.com/docs/latest/HTML/Content/Authoring/SQLReferenceManual/Statements/SELECT/table-ref.htm

    // This regex captures literal sources with a schema defined, ex: schema."table.name"
    val literalSourceWithSchema = "\\.\".*\"".r
    def noSchemaFound(source: String): Boolean = if (source.contains("\"")) {
      literalSourceWithSchema.findFirstIn(source).isEmpty
    } else {
      source.split("\\.").length < 2
    }

    def appendSchema(dbSchema: String, source: String): String = {
      if (source.startsWith("(")) {
        // The source could be sub-query, in which case we don't append the schema
        source
      } else {
        if (noSchemaFound(source)) {
          s"$dbSchema." + source
        } else {
          source
        }
      }
    }

    /**
     * Recursion to locate the FROM clause for processing.
     * */
    @tailrec
    def addDbSchemaToQueryRecursion(parts: List[String], dbSchema: String, query: String, foundFROMClause: Boolean): String = {
      parts.headOption match {
        case Some(head) =>
          if(head.toLowerCase == "from") {
            addDbSchemaToQueryRecursion(parts.tail, dbSchema, query + s" $head", foundFROMClause = true)
          } else if (foundFROMClause) {
            val dataSource = appendSchema(dbSchema, head)
            query + s" $dataSource " + parts.tail.mkString(" ")
          } else {
            addDbSchemaToQueryRecursion(parts.tail, dbSchema, query + s" $head", foundFROMClause)
          }
        case None => query
      }
    }

    /**
     * Given a query, we need to located the FROM clause and append the schema to the table. The query could contains
     * sub queries, string literals, or already defined a schema, all of which needs to be handled.
     * */
    val queryParts = query.split(" ").toList
    dbSchema match {
      case Some(schema) => addDbSchemaToQueryRecursion(queryParts, schema, "", foundFROMClause = false).trim
      case None => query
    }
  }
}

/**
 * A SchemaTools extension specifically for Vertica 10.x. Because Vertica 10 report Complex types as VARCHAR,
 * SchemaToolsV10 will intercept super().getColumnInfo() calls to check for complex types through queries to Vertica.
 * */
class SchemaToolsV10 extends SchemaTools {

  override def getColumnInfo(jdbcLayer: JdbcLayerInterface, tableSource: TableSource): ConnectorResult[Seq[ColumnDef]] = {
    super.getColumnInfo(jdbcLayer, tableSource) match {
      case Left(err) => Left(err)
      case Right(colList) =>
        colList.map(col =>
          tableSource match {
            case tb: TableName =>
              val unQuotedName = tb.getTableName.replaceAll("\"", "")
              val unQuotedDbSchema = tb.getDbSchema.replaceAll("\"", "")
              checkForComplexType(col, unQuotedName, unQuotedDbSchema, jdbcLayer)
            case TableQuery(_,_,_) => Right(col)
          }
        )
          .toList
          .traverse(_.leftMap(err => NonEmptyList.one(err)).toValidated).toEither
          .map(list => list)
          .left.map(errors => ErrorList(errors))
    }
  }

  /**
   * Vertica 10 reports complex types as string type over JDBC. Thus, we need to check if the jdbc type is a Spark
   * string type, then we check if it is complex type.
   * */
  private def checkForComplexType(col: ColumnDef, tableName: String, dbSchema: String, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    super.getCatalystTypeFromJdbcType(col.jdbcType, 0, 0, false, "") match {
      case Right(dataType) => dataType match {
        case StringType => checkV10ComplexType(col, tableName, dbSchema, jdbcLayer)
        case _ => Right(col)
      }
      case Left(_) => Right(col)
    }
  }

  /**
   * We check to see if the column is actually a complex type in Vertica 10. If so, then we return a complex type
   * ColumnDef. This ColumnDef is not correct; It is only meant to mark the column as complex type so we can return
   * an error later on.
   *
   * If column is not of complex type in Vertica, then return the ColumnDef as is.
   * */
  private def checkV10ComplexType(colDef: ColumnDef, tableName: String, dbSchema: String, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    val complexTypesTable = new ComplexTypesTable(jdbcLayer)
    new ColumnsTable(jdbcLayer).getColumnInfo(colDef.label, tableName, dbSchema) match {
      case Left(value) => Left(value)
      case Right(columnInfo: ColumnInfo) =>
        val verticaType = columnInfo.verticaType
        val metadata = new MetadataBuilder().putLong(MetadataKey.DEPTH, 0).build()

        // Native array case
        if(verticaType > VERTICA_NATIVE_ARRAY_BASE_ID && verticaType < VERTICA_SET_MAX_ID) {
          val dummyChild = ColumnDef("", java.sql.Types.VARCHAR, "STRING", 0, 0, false, false, metadata)
          Right(colDef.copy(jdbcType = java.sql.Types.ARRAY, children = List(dummyChild)))
        }
        // Complex array case
        else {
          complexTypesTable.findComplexTypeInfo(verticaType) match {
            // If found, we return a struct regardless of the actual CT type.
            case Right(_) => Right(colDef.copy(jdbcType = java.sql.Types.STRUCT, metadata = metadata))
            // Else, return the column def as is
            case Left(_) => Right(colDef)
          }
        }
    }
  }

}

