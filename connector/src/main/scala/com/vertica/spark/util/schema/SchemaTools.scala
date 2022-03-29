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
import com.vertica.spark.util.error.ErrorHandling.{ConnectorResult, SchemaResult}
import com.vertica.spark.util.error._
import com.vertica.spark.util.schema.SchemaTools.{VERTICA_NATIVE_ARRAY_BASE_ID, VERTICA_PRIMITIVES_MAX_ID, VERTICA_SET_BASE_ID, VERTICA_SET_MAX_ID}
import org.apache.spark.sql.types._

import java.sql.ResultSetMetaData
import scala.annotation.tailrec
import scala.util.{Either, Try}
import scala.util.control.Breaks.{break, breakable}

case class ColumnDef(
                      label: String,
                      colType: Int,
                      colTypeName: String,
                      size: Int,
                      scale: Int,
                      signed: Boolean,
                      nullable: Boolean,
                      metadata: Metadata,
                      childDefinitions: List[ColumnDef] = Nil
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
   * @param jdbcLayer Depedency for communicating with Vertica over JDBC
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
   * Converts spark schema to table column defs in Vertica format
   *
   * @param schema Schema in spark format
   * @return List of column names and types, that can be used in a Vertica CREATE TABLE.
   * */
  def makeTableColumnDefs(schema: StructType, strlen: Long, jdbcLayer: JdbcLayerInterface, arrayLength: Long): ConnectorResult[String]

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
}

object SchemaTools {
  //  This number is chosen from diff of array and set base id.
  val VERTICA_NATIVE_ARRAY_BASE_ID: Long = 1500L
  val VERTICA_SET_BASE_ID: Long = 2700L
  // This number is not defined be Vertica, so we use the delta of set and native array base id.
  val VERTICA_PRIMITIVES_MAX_ID:Long = 1200L
  val VERTICA_SET_MAX_ID: Long = 2700L + VERTICA_PRIMITIVES_MAX_ID
}

class SchemaTools extends SchemaToolsInterface {
  private val logger = LogProvider.getLogger(classOf[SchemaTools])
  private val unknown = "UNKNOWN"
  private val maxlength = "maxlength"
  private val longlength = 65000

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
      case _ => getCatalystTypeFromJdbcType(sqlType, precision, scale, signed, typename)
    }
  }

  private def getArrayType(elementDef: List[ColumnDef]): Either[SchemaError, ArrayType] = {
    @tailrec
    def makeNestedArrays(arrayDepth: Long, arrayElement: DataType): DataType = {
      if(arrayDepth > 0) {
        makeNestedArrays(arrayDepth - 1, ArrayType(arrayElement))
      } else {
        arrayElement
      }
    }

    elementDef.headOption match {
      case Some(element) =>
        getCatalystTypeFromJdbcType(element.colType, element.size, element.scale, element.signed, element.colTypeName) match {
          case Right(elementType) => Right(ArrayType(makeNestedArrays(element.metadata.getLong(MetadataKey.DEPTH), elementType)))
          case Left(err) => Left(ArrayElementConversionError(err.sqlType, err.typename))
        }
      case None => Left(MissingElementTypeError())
    }
  }

  private def getCatalystTypeFromJdbcType(sqlType: Int,
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
      case java.sql.Types.STRUCT => StringType
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
    val tableName = tableSource match {
      case tableName: TableName => tableName.getFullTableName.replaceAll("\"","")
      case _ => ""
    }

    this.getColumnInfo(jdbcLayer, tableSource) match {
      case Left(err) => Left(err)
      case Right(colInfo) =>
        val errorsOrFields: List[Either[SchemaError, StructField]] = colInfo.map(info => {
          this.getCatalystType(info.colType, info.size, info.scale, info.signed, info.colTypeName, info.childDefinitions)
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
    // Query for an empty result set from Vertica.
    // This is simply so we can load the metadata of the result set
    // and use this to retrieve the name and type information of each column
    val (tableName, query) = tableSource match {
      case tb: TableName =>
        (tb.getFullTableName, "SELECT * FROM " + tb.getFullTableName + " WHERE 1=0")
      case TableQuery(query, _) =>
        ("", "SELECT * FROM (" + query + ") AS x WHERE 1=0")
    }

    jdbcLayer.query(query) match {
      case Left(err) => Left(JdbcSchemaError(err))
      case Right(rs) =>
        try {
          val rsmd = rs.getMetaData
          val colDefSeq: Seq[ColumnDef] = (1 to rsmd.getColumnCount).map(idx => {
            val columnLabel = rsmd.getColumnLabel(idx)
            val typeName = rsmd.getColumnTypeName(idx)
            val fieldSize = DecimalType.MAX_PRECISION
            val fieldScale = rsmd.getScale(idx)
            val isSigned = rsmd.isSigned(idx)
            val nullable = rsmd.isNullable(idx) != ResultSetMetaData.columnNoNulls
            val metadata = new MetadataBuilder().putString(MetadataKey.NAME, columnLabel).build()
            val colType = rsmd.getColumnType(idx)
            val colDef = ColumnDef(columnLabel, colType, typeName, fieldSize, fieldScale, isSigned, nullable, metadata)
            checkForComplexType(colDef, tableName, jdbcLayer) match {
              case Right(columnDef) => columnDef
              case Left(err) => err match {
                case e: VerticaComplexTypeNotFound => throw e
                case _ => throw new RuntimeException(err.getFullContext)
              }
            }
          })
          Right(colDefSeq)
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

  private def checkForComplexType(colDef: ColumnDef, tableName: String, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    colDef.colType match {
      case java.sql.Types.ARRAY |
           java.sql.Types.STRUCT => queryColumnDef(colDef, tableName, jdbcLayer)
      case _ => Right(colDef)
    }
  }

  /**
   * For complex types, JDBC metadata does not contains information about their elements, but they are available in
   * Vertica systems tables. This function takes a ColumnDef of a complex type and injects it corresponding element
   * ColumnDefs through a series of JDBC queries to Vertica system tables.
   * */
  private def queryColumnDef(complexTypeColDef: ColumnDef, tableName: String, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    val table = tableName.replace("\"", "")
    val queryColType = s"SELECT data_type_id, data_type FROM columns WHERE table_name='$table' AND column_name='${complexTypeColDef.label}'"
    // We first query from column table for column's Vertica type. Note that data_type_id is Vertica's internal type id, not JDBC.
    JdbcUtils.queryAndNext(queryColType, jdbcLayer, (rs) => {
      val verticaType = rs.getLong("data_type_id")
      val typeName = getTypeName(rs.getString("data_type"))
      complexTypeColDef.colType match {
        case java.sql.Types.ARRAY => makeArrayColumnDef(complexTypeColDef, verticaType, jdbcLayer)
        // Todo: implement Row support
        case java.sql.Types.STRUCT => Left(MissingSqlConversionError(complexTypeColDef.colType.toString, typeName))
        case _ => Left(MissingSqlConversionError(complexTypeColDef.colType.toString, typeName))
      }
    })
  }

  private def getTypeName(dataType:String) : String = {
    dataType
      .replace('[',',')
      .replace(']',',')
      .split(',')(0)
  }

  /**
   * Query Vertica system tables to fill in an array ColumnDefs with it's elements.
   * */
  private def makeArrayColumnDef(arrayColDef: ColumnDef, verticaTypeId: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    /**
     * A 1D primitive array is considered a Native type by Vertica. Their type information is tracked in types table.
     * Else, nested arrays or arrays with complex elements are tracked in complex_types table.
     * We could infer from the vertica id if it is a native type or not.
     * */
    // Native array id = 1500 + primitive type id
    var elementId = verticaTypeId - VERTICA_NATIVE_ARRAY_BASE_ID
    // Sets are also tracked in types table
    val isSet = elementId > VERTICA_PRIMITIVES_MAX_ID && elementId < VERTICA_SET_MAX_ID
    // Set id = 2700 + primitive type id
    if (isSet) elementId = verticaTypeId - VERTICA_SET_BASE_ID
    val isNativeArray = elementId < VERTICA_PRIMITIVES_MAX_ID
    val elementDef = if (isNativeArray) queryVerticaPrimitiveDef(elementId, 0, jdbcLayer)
    else getNestedArrayElementDef(verticaTypeId, jdbcLayer)
    fillArrayColumnDef(arrayColDef, elementDef, isSet)
  }

  private def fillArrayColumnDef(srcArrayDef: ColumnDef, elementDef: ConnectorResult[ColumnDef], isVerticaSet: Boolean): ConnectorResult[ColumnDef] =
    elementDef match {
      case Right(element) =>
        val metaData = new MetadataBuilder()
          .putString(MetadataKey.NAME, srcArrayDef.label)
          .putBoolean(MetadataKey.IS_VERTICA_SET, isVerticaSet)
          .putLong(MetadataKey.DEPTH, element.metadata.getLong(MetadataKey.DEPTH))
          .build
        Right(srcArrayDef.copy(childDefinitions = List(element), metadata = metaData))
      case Left(err) => Left(err)
    }

  private def getNestedArrayElementDef(verticaType: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    /**
     * complex_types table records all complex types created in Vertica.
     * Each row has a field_id linking and the complex type to it's child. For a nested array, each nested element
     * is recorded in the table and its field_id points to it's child.
     *
     * The recursion below will start from the top complex type and follow the field_id to locate the element type
     * of a nested array.
     */
    @tailrec
    def getNestedElementDef(verticaType: Long, jdbcLayer: JdbcLayerInterface, depth: Int): ConnectorResult[ColumnDef] = {
      val queryComplexType = s"SELECT field_type_name, type_id ,field_id, numeric_scale FROM complex_types WHERE type_id='$verticaType'"
      jdbcLayer.query(queryComplexType) match {
        // Because this is a tailrec, we can't use finally block
        case Right(rs) =>
          if (rs.next()) {
            val fieldTypeName = rs.getString("field_type_name")
            val verticaType = rs.getLong("field_id")
            rs.close()
            // complex type name starts with _ct_
            if (fieldTypeName.startsWith("_ct_")) {
              getNestedElementDef(verticaType, jdbcLayer, depth + 1)
            } else {
              // Once the element type is found, query from types table
              queryVerticaPrimitiveDef(verticaType, depth, jdbcLayer)
            }
          } else {
            rs.close()
            Left(VerticaComplexTypeNotFound(verticaType))
          }
        case Left(error) => Left(error)
      }
    }

    getNestedElementDef(verticaType, jdbcLayer, 0)
  }

  private def queryVerticaPrimitiveDef(verticaType: Long, depth: Int, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    val queryNativeTypes = s"SELECT type_id, jdbc_type, type_name FROM types WHERE type_id=$verticaType"
    JdbcUtils.queryAndNext(queryNativeTypes, jdbcLayer,
      (rs) =>{
        val jdbcType = rs.getLong("jdbc_type").toInt
        val typeName = rs.getString("type_name")
        Right(makeArrayElementDef(jdbcType, typeName, depth))
      },
      (_) => Left(VerticaNativeTypeNotFound(verticaType)))
  }

  private def makeArrayElementDef(jdbcType: Int, typeName: String, depth: Int) = {
    val sqlType = jdbcType
    val fieldSize = DecimalType.MAX_PRECISION
    val fieldScale = 0
    val isSigned = true
    val nullable = 1 != ResultSetMetaData.columnNoNulls
    val metadata = new MetadataBuilder()
      .putString(MetadataKey.NAME, "element")
      .putLong(MetadataKey.DEPTH, depth)
      .build()
    ColumnDef("element", sqlType, typeName, fieldSize, fieldScale, isSigned, nullable, metadata)
  }

  override def getVerticaTypeFromSparkType(sparkType: DataType, strlen: Long, arrayLength: Long, metadata: Metadata = Metadata.empty): SchemaResult[String] = {
    sparkType match {
      // To be reconsidered. Store as binary for now
      case org.apache.spark.sql.types.MapType(_,_,_) |
           org.apache.spark.sql.types.StructType(_) => Right("VARBINARY(" + longlength + ")")
      case org.apache.spark.sql.types.ArrayType(sparkType,_) => sparkArrayToVerticaArray(sparkType, strlen, arrayLength, metadata)
      case _ => this.sparkPrimitiveToVerticaPrimitive(sparkType, strlen)
    }
  }

  private def sparkArrayToVerticaArray(dataType: DataType, strlen: Long, arrayLength: Long, metadata: Metadata): SchemaResult[String] = {
    val length = if (arrayLength <= 0) "" else s",$arrayLength"
    val isArraySetType = Try{metadata.getBoolean(MetadataKey.IS_VERTICA_SET)}.getOrElse(false)
    val keyword = if(isArraySetType) "SET" else "ARRAY"

    @tailrec
    def recursion(dataType: DataType, leftAccumulator: String, rightAccumulator: String, depth: Int): SchemaResult[String] = {
      dataType match {
        case ArrayType(elementType, _) =>
          recursion(elementType, s"$leftAccumulator$keyword[", s"$length]$rightAccumulator", depth + 1)
        case _ =>
          this.sparkPrimitiveToVerticaPrimitive(dataType, strlen) match {
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
      case org.apache.spark.sql.types.DecimalType() => Right("DECIMAL")
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
      case _ => Left(MissingSparkConversionError(sparkType))
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

  private def castToVarchar: String => String = colName => colName + "::varchar AS " + addDoubleQuotes(colName)

  def makeColumnsString(columnDefs: Seq[ColumnDef], requiredSchema: StructType): String = {
    val requiredColumnDefs: Seq[ColumnDef] = if (requiredSchema.nonEmpty) {
      columnDefs.filter(cd => requiredSchema.fields.exists(field => field.name == cd.label))
    } else {
      columnDefs
    }

    requiredColumnDefs.map(info => {
      info.colType match {
        case java.sql.Types.OTHER =>
          val typenameNormalized = info.colTypeName.toLowerCase()
          if (typenameNormalized.startsWith("interval") ||
            typenameNormalized.startsWith("uuid")) {
            castToVarchar(info.label)
          } else {
            addDoubleQuotes(info.label)
          }
        case java.sql.Types.TIME => castToVarchar(info.label)
        case java.sql.Types.ARRAY =>
          val isArraySetType = Try{info.metadata.getBoolean(MetadataKey.IS_VERTICA_SET)}.getOrElse(false)
          // Casting on Vertica side as a work around until Vertica Export supports Set
          if(isArraySetType) castToArray(info) else info.label
        case _ => addDoubleQuotes(info.label)
      }
    }).mkString(",")
  }

  private def castToArray(colInfo: ColumnDef) = {
    val label = colInfo.label
    val elementType = colInfo.childDefinitions.headOption match {
       case Some(elementDef) => elementDef.colTypeName
       case None => "UNKNOWN"
    }
    s"(${label}::ARRAY[${elementType}]) as $label"
  }

  def makeTableColumnDefs(schema: StructType, strlen: Long, jdbcLayer: JdbcLayerInterface, arrayLength: Long): ConnectorResult[String] = {
    val sb = new StringBuilder()

    sb.append(" (")
    var first = true
    schema.foreach(s => {
      logger.debug("colname=" + "\"" + s.name + "\"" + "; type=" + s.dataType + "; nullable=" + s.nullable)
      if (!first) {
        sb.append(",\n")
      }
      first = false
      sb.append("\"" + s.name + "\" ")

      // remains empty unless we have a DecimalType with precision/scale
      var decimal_qualifier: String = ""
      if (s.dataType.toString.contains("DecimalType")) {

        // has precision only
        val p = "DecimalType\\((\\d+)\\)".r
        if (s.dataType.toString.matches(p.toString)) {
          val p(prec) = s.dataType.toString
          decimal_qualifier = "(" + prec + ")"
        }

        // has precision and scale
        val ps = "DecimalType\\((\\d+),(\\d+)\\)".r
        if (s.dataType.toString.matches(ps.toString)) {
          val ps(prec, scale) = s.dataType.toString
          decimal_qualifier = "(" + prec + "," + scale + ")"
        }
      }

      for {
        col <- getVerticaTypeFromSparkType(s.dataType, strlen, arrayLength, s.metadata) match {
          case Left(err) =>
            return Left(SchemaConversionError(err).context("Schema error when trying to create table"))
          case Right(datatype) =>
            Right(datatype + decimal_qualifier)
        }
        _ = sb.append(col)
        _ = if (!s.nullable) {
          sb.append(" NOT NULL")
        }
      } yield ()
    })

    sb.append(")")
    Right(sb.toString)
  }

  def getMergeInsertValues(jdbcLayer: JdbcLayerInterface, tableName: TableName, copyColumnList: Option[ValidColumnList]): ConnectorResult[String] = {
    val valueList = getColumnInfo(jdbcLayer, tableName) match {
      case Right(info) => Right(info.map(x => "temp." + addDoubleQuotes(x.label)).mkString(","))
      case Left(err) => Left(JdbcSchemaError(err))
    }
    valueList
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
      case field if(addDoubleQuotes(field.name) == colName) =>
        if (field.metadata.contains(maxlength) && field.dataType.simpleString == "string") {
          if(field.metadata.getLong(maxlength) > longlength) "long varchar(" + field.metadata.getLong(maxlength).toString + ")"
          else "varchar(" + field.metadata.getLong(maxlength).toString + ")"
        }
        else if(field.metadata.contains(maxlength) && field.dataType.simpleString == "binary"){
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
    }

    else {
      col
    }
  }

  def inferExternalTableSchema(createExternalTableStmt: String, schema: StructType, tableName: String, strlen: Long, arrayLength: Long): ConnectorResult[String] = {
    val stmt = createExternalTableStmt.replace("\"" + tableName + "\"", tableName)
    val indexOfOpeningParantheses = stmt.indexOf("(")
    val indexOfClosingParantheses = stmt.indexOf(")")
    val schemaString = stmt.substring(indexOfOpeningParantheses + 1, indexOfClosingParantheses)
    val schemaList = schemaString.split(",").toList

    val updatedSchema: String = schemaList.map(col => {
      val indexOfFirstDoubleQuote = col.indexOf("\"")
      val indexOfSpace = col.indexOf(" ", indexOfFirstDoubleQuote)
      val colName = col.substring(indexOfFirstDoubleQuote, indexOfSpace)

      if(schema.nonEmpty){
        updateFieldDataType(col, colName, schema, strlen, arrayLength)
      }
      else if(col.toLowerCase.contains("varchar")) colName + " varchar(" + strlen + ")"
      else if(col.toLowerCase.contains("varbinary")) colName + " varbinary(" + longlength + ")"
      else col
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
}
