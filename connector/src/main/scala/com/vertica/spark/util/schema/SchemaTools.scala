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

import com.vertica.spark.datasource.jdbc._
import org.apache.spark.sql.types.{MetadataBuilder, _}

import java.sql.{ResultSet, ResultSetMetaData}
import cats.data.NonEmptyList
import cats.implicits._
import com.vertica.dataengine.{ColumnDescription, ComplexTypeColumnDescription}
import com.vertica.dsi.dataengine.interfaces.IColumn

import scala.util.{Either, Failure, Success, Try}
import com.vertica.spark.config.{LogProvider, TableName, TableQuery, TableSource, ValidColumnList}
import com.vertica.spark.util.error.ErrorHandling.{ConnectorResult, SchemaResult}
import com.vertica.spark.util.error._
import com.vertica.spark.util.version.VerticaVersionUtils
import org.apache.commons.lang.StringUtils

import java.util
import scala.annotation.tailrec
import scala.collection.JavaConverters.asScalaBufferConverter
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
   * @return String representing Vertica type, that one could use in a create table statement
   */
  def getVerticaTypeFromSparkType (sparkType: org.apache.spark.sql.types.DataType, strlen: Long, arrlen: Long): SchemaResult[String]

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
  def makeTableColumnDefs(schema: StructType, strlen: Long, jdbcLayer: JdbcLayerInterface, arrlen: Long): ConnectorResult[String]

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
  def inferExternalTableSchema(createExternalTableStmt: String, schema: StructType, tableName: String, strlen: Long, arrlen: Long): ConnectorResult[String]
}

class SchemaTools extends SchemaToolsInterface {
  private val logger = LogProvider.getLogger(classOf[SchemaTools])
  private val unknown = "UNKNOWN"
  private val maxlength = "maxlength"
  private val longlength = 65000

  private def addDoubleQuotes(str: String): String = {
    "\"" + str + "\""
  }

  // scalastyle:off
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
    elementDef.headOption match {
      case Some(element) =>
        getCatalystTypeFromJdbcType(element.colType, element.size, element.scale, element.signed, element.colTypeName) match {
          case Right(elementType) => Right(ArrayType(makeNestedArrays(element.metadata.getLong("depth"), elementType)))
          case Left(err) => Left(ArrayElementConversionError(err.sqlType, err.typename))
        }
      case None => Left(MissingElementTypeError())
    }
  }

  @tailrec
  private def makeNestedArrays(arrayDepth: Long, arrayElement: DataType): DataType = {
    if(arrayDepth > 0)
      makeNestedArrays(arrayDepth - 1, ArrayType(arrayElement))
    else
      arrayElement
  }

  def getCatalystTypeFromJdbcType(jdbcType: Int,
                                  precision: Int,
                                  scale: Int,
                                  signed: Boolean,
                                  typename: String): Either[MissingSqlConversionError, DataType] = {

    val answer = jdbcType match {
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

    if (answer == null) Left(MissingSqlConversionError(jdbcType.toString, typename))
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
    var tableName = ""
    val query = tableSource match {
      case tablename: TableName =>
        tableName =  tablename.getFullTableName
        "SELECT * FROM " + tablename.getFullTableName + " WHERE 1=0"
      case TableQuery(query, _) => "SELECT * FROM (" + query + ") AS x WHERE 1=0"
    }

    jdbcLayer.query(query) match {
      case Left(err) => Left(JdbcSchemaError(err))
      case Right(rs) =>
        try {
          val rsmd = rs.getMetaData
          Right((1 to rsmd.getColumnCount).map(idx => {
            val columnLabel = rsmd.getColumnLabel(idx)
            val typeName = rsmd.getColumnTypeName(idx)
            val fieldSize = DecimalType.MAX_PRECISION
            val fieldScale = rsmd.getScale(idx)
            val isSigned = rsmd.isSigned(idx)
            val nullable = rsmd.isNullable(idx) != ResultSetMetaData.columnNoNulls
            val metadata = new MetadataBuilder().putString("name", columnLabel).build()
            val colType = rsmd.getColumnType(idx)
            // Ideally, we do not want to do this check.
            if(colType == java.sql.Types.ARRAY) {
              val arrayColDef: ColumnDef = getArrayColumnDef(rsmd, columnLabel)
              //checkArrayCompatibility(jdbcLayer, arrayColDef.metadata.getLong("depth"), columnLabel)
              val arrayDef = ColumnDef(columnLabel, colType,typeName, fieldSize, fieldScale, isSigned, nullable, metadata)
              val res = getArrayColumnDef(arrayDef, tableName, jdbcLayer)
              print(res)
              res.getOrElse(null)
            } else
              ColumnDef(columnLabel, colType, typeName, fieldSize, fieldScale, isSigned, nullable, metadata)
          }))
        }
        catch {
          case e: ArrayNotSupported => Left(e)
          case e: NestedArrayNotSupported => Left(e)
          case e: Throwable =>
            Left(DatabaseReadError(e).context("Could not get column info"))
        }
        finally {
          rs.close()
        }
    }
  }

  private def getArrayColumnDef(arrayDef: ColumnDef, tableName: String, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef] = {
    val colName = arrayDef.label
    val query = s"SELECT data_type_id FROM columns WHERE table_name=${tableName.replace("\"", "'")} AND column_name='$colName'"
    for {
      rs <- queryAndNext(query, jdbcLayer)
      verticaType = rs.getLong(1)
      arrayDef <- getArrayDef(arrayDef, jdbcLayer, verticaType)
    } yield arrayDef
  }

  private def getArrayDef(arrayDef: ColumnDef, jdbcLayer: JdbcLayerInterface, verticaType: Long): ConnectorResult[ColumnDef] = {
    val verticaTypeId = verticaType - 1500;
    val query = s"SELECT jdbc_type, type_name FROM types WHERE type_id=$verticaTypeId"
    jdbcLayer.query(query) match {
      case Right(rs) =>
        if(rs.next){
          rs.next
         getElementDef(jdbcLayer, 0, verticaTypeId) match {
            case Right(elementDef) =>
              val metaData = new MetadataBuilder()
                .putString("name", arrayDef.label)
                .putLong("depth", elementDef.metadata.getLong("depth"))
                .build
              Right(arrayDef.copy(childDefinitions = List(elementDef), metadata = metaData))
            case Left(err) => Left(err)
          }
        } else {
          getNestedElementDef(verticaType, jdbcLayer) match{
            case Right(elementDef) =>
              val metaData = new MetadataBuilder()
                .putString("name", arrayDef.label)
                .putLong("depth", elementDef.metadata.getLong("depth"))
                .build
             Right(arrayDef.copy(childDefinitions = List(elementDef), metadata = metaData))
            case Left(err) => Left(err)
          }
        }
      case Left(error) => Left(error)
    }
  }

  private def queryAndNext(query: String, jdbcLayer: JdbcLayerInterface): ConnectorResult[ResultSet] ={
    jdbcLayer.query(query) match {
      case Right(rs) => rs
        if(rs.next)
          Right(rs)
        else
          Left(EmptyQueryError(query))
      case Left(rs) => Left(rs)
    }
  }

  case class EmptyQueryError(query: String) extends ConnectorError{
    def getFullContext: String = s"Query result is empty \n QUERY:[$query] "
  }

  private def getNestedElementDef(verticaType: Long, jdbcLayer: JdbcLayerInterface): ConnectorResult[ColumnDef]  = {
   @tailrec
   def recursion(verticaType: Long, jdbcLayer: JdbcLayerInterface, depth: Int): ConnectorResult[ColumnDef] = {
     val query = s"SELECT field_type_name, type_id ,field_id, numeric_scale FROM complex_types WHERE type_id='$verticaType'"
     jdbcLayer.query(query) match {
       case Right(rs) =>
         rs.next
         val fieldTypeName = rs.getString(1)
         val verticaType = rs.getLong("field_id")
         if(fieldTypeName.startsWith("_ct_")){
           recursion(verticaType, jdbcLayer, depth + 1)
         }else{
           getElementDef(jdbcLayer, depth, verticaType)
         }
       case Left(e) => Left(e)
     }
   }
    Try{recursion(verticaType,jdbcLayer, 0)}
    match {
      case Success(res) => res
      case Failure(exception) => throw exception
    }
  }

  private def getElementDef(jdbcLayer: JdbcLayerInterface, depth: Int, verticaType: Long): ConnectorResult[ColumnDef] = {
    val fieldSize = DecimalType.MAX_PRECISION
    val fieldScale = 0
    val isSigned = true
    val nullable = 1 != ResultSetMetaData.columnNoNulls
    val metadata = new MetadataBuilder()
      .putString("name", "element")
      .putLong("depth", depth)
      .build()
    val query = s"SELECT jdbc_type, type_name FROM types WHERE type_id=$verticaType"
    jdbcLayer.query(query) match {
      case Right(rs) =>
        rs.next
        val sqlType = rs.getLong("jdbc_type").toInt
        val typeName = rs.getString("type_name")
        Right(ColumnDef("element", sqlType, typeName, fieldSize, fieldScale, isSigned, nullable, metadata))
      case Left(error) => Left(error)
    }
  }

  private def checkArrayCompatibility(jdbcLayer: JdbcLayerInterface, arrayDepth: Long, columnLabel: String): Unit = {
    val verticaVersion = VerticaVersionUtils.get(jdbcLayer)
    if (verticaVersion.major < 10)
      throw ArrayNotSupported(columnLabel)
    if (arrayDepth > 0 && verticaVersion.major <= 11)
      throw NestedArrayNotSupported(columnLabel)
  }

  case class ArrayNotSupported(colName: String) extends RuntimeException with SchemaError {
    override def getFullContext: String = s"Column $colName is of array type. Array requires in Vertica version 10 more higher."
  }

  case class NestedArrayNotSupported(colName: String) extends RuntimeException with SchemaError {
    override def getFullContext: String = s"Column $colName is has type nested array. Nested array is currently not supported by Vertica."
  }

  /**
   * Currently, the data we need to reconstruct array is hidden behind Vertica JDBC API.
   * Using reflection, we can reach the private field we want.
   * Ideally, Vertica should provide these data to us in a future JDBC release.
   * */
  private def getArrayColumnDef(rsmd: ResultSetMetaData, colName: String): ColumnDef ={
    //  Hardcoded. Located the correct super class and get the private field
    val field = rsmd.getClass.getSuperclass.getSuperclass.getDeclaredField("m_columnMetaData")
    field.setAccessible(true)
    val columnMetaDataField = field.get(rsmd).asInstanceOf[util.ArrayList[IColumn]].asScala.toList
    val colMetaData = columnMetaDataField.find(_.getName.equals(colName)) match {
      case Some(data) => data.asInstanceOf[ColumnDescription]
      case None => throw new RuntimeException("Reflection error: finding array metadata")
    }
    // Vertica's nested structure are stored as child columns.
    val elementDef = getElementDef(colMetaData.getChildColumnIterator.next(), 0)
    val columnLabel = colMetaData.getLabel
    val metadata = new MetadataBuilder()
      .putString("name", columnLabel)
      .putLong("depth", elementDef.metadata.getLong("depth"))
      .build()
    ColumnDef(colMetaData.getLabel, java.sql.Types.ARRAY, "", 0, 0, elementDef.signed, elementDef.nullable, metadata, List(elementDef))
  }

  @tailrec
  private final def getElementDef(colDesc: ColumnDescription, depth: Int): ColumnDef ={
    colDesc match {
      case desc: ComplexTypeColumnDescription => getElementDef(desc.getChildColumnIterator.next(), depth + 1)
      case desc: ColumnDescription =>
        val columnLabel = desc.getLabel
        val typeName = desc.getTypeMetadata.getTypeName
        val fieldSize = DecimalType.MAX_PRECISION
        val fieldScale =  desc.getTypeMetadata.getScale
        val isSigned =desc.getTypeMetadata.isSigned
        val nullable = 1 != ResultSetMetaData.columnNoNulls
        val metadata = new MetadataBuilder()
          .putString("name", columnLabel)
          .putLong("depth", depth.toLong)
          .build()
        val colType = desc.getSQLType
        ColumnDef(columnLabel, colType,typeName,fieldSize,fieldScale,isSigned,nullable,metadata)
      case _ => throw new RuntimeException("Reflection error finding array element type info")
    }
  }

  override def getVerticaTypeFromSparkType (sparkType: org.apache.spark.sql.types.DataType, strlen: Long, arrlen: Long): SchemaResult[String] = {
    sparkType match {
      // To be reconsidered. Store as binary for now
      case org.apache.spark.sql.types.MapType(_,_,_) |
           org.apache.spark.sql.types.StructType(_) => Right("VARBINARY(" + longlength + ")")
      case org.apache.spark.sql.types.ArrayType(sparkType,_) => sparkArrayToVerticaArray(sparkType, strlen, arrlen)
      case _ => this.sparkPrimitiveToVerticaPrimitive(sparkType, strlen)
    }
  }

  private def sparkArrayToVerticaArray(dataType: DataType, strlen: Long, arrlen: Long): SchemaResult[String] = {
    val length = if(arrlen <=0) "" else s", $arrlen"
    @tailrec
      def recursion(dataType: DataType, leftAccumulator: String, rightAccumulator:String, depth: Int):SchemaResult[String] = {
          dataType match {
            case ArrayType(elementType, _) =>
              recursion(elementType, s"${leftAccumulator}ARRAY[", s"$length]$rightAccumulator", depth+1)
            case _ =>
              this.sparkPrimitiveToVerticaPrimitive(dataType, strlen) match {
                case Right(verticaType) =>
                  Right(s"$leftAccumulator$verticaType$rightAccumulator")
                case Left(error) => Left(error)
              }
          }
      }
    recursion(dataType,"ARRAY[",s"$length]",0)
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
        case _ => addDoubleQuotes(info.label)
      }
    }).mkString(",")
  }

  def makeTableColumnDefs(schema: StructType, strlen: Long, jdbcLayer: JdbcLayerInterface, arrlen: Long): ConnectorResult[String] = {
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
        col <- getVerticaTypeFromSparkType(s.dataType, strlen, arrlen) match {
          case Left(err) =>
            return Left(SchemaConversionError(err).context("Schema error when trying to create table"))
          case Right(datatype) =>
            val depth = StringUtils.countMatches(datatype, "ARRAY")
            checkArrayCompatibility(jdbcLayer, depth - 1, s.name)
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

  def updateFieldDataType(col: String, colName: String, schema: StructType, strlen: Long, arrlen: Long): String = {
    val fieldType = schema.collect {
      case field if(addDoubleQuotes(field.name) == colName) =>
        if (field.metadata.contains(maxlength) && field.dataType.simpleString == "string") {
          if(field.metadata.getLong(maxlength) > longlength) "long varchar(" + field.metadata.getLong(maxlength).toString + ")"
          else "varchar(" + field.metadata.getLong(maxlength).toString + ")"
        }
        else if(field.metadata.contains(maxlength) && field.dataType.simpleString == "binary"){
          "varbinary(" + field.metadata.getLong(maxlength).toString + ")"
        }
        else  {
          getVerticaTypeFromSparkType(field.dataType, strlen, arrlen) match {
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

  def inferExternalTableSchema(createExternalTableStmt: String, schema: StructType, tableName: String, strlen: Long, arrlen: Long): ConnectorResult[String] = {
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
        updateFieldDataType(col, colName, schema, strlen, arrlen)
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
