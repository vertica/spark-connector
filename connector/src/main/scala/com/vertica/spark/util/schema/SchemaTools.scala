package com.vertica.spark.util.schema

import com.vertica.spark.datasource.jdbc._
import org.apache.spark.sql.types._
import java.sql.ResultSetMetaData

import com.vertica.spark.util.error._
import com.vertica.spark.util.error.SchemaErrorType._

trait SchemaToolsInterface {
  def readSchema(jdbcLayer: JdbcLayerInterface, tablename: String): Either[Seq[SchemaError], StructType]
}

class SchemaTools extends SchemaToolsInterface {
  private def getCatalystType(
    sqlType: Int,
    precision: Int,
    scale: Int,
    signed: Boolean,
    typename: String): Either[SchemaError, DataType] = {
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
      case java.sql.Types.DECIMAL if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.DECIMAL => DecimalType(DecimalType.USER_DEFAULT.precision,DecimalType.USER_DEFAULT.scale)
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
      case java.sql.Types.OTHER => if(typename.toLowerCase().startsWith("interval")) StringType else null
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
    var errList = List[SchemaError]()
    var schema : Option[StructType] = None

    // Query for an empty result set from Vertica.
    // This is simply so we can load the metadata of the result set
    // and use this to retrieve the name and type information of each column
    jdbcLayer.query("SELECT * FROM " + tablename + " WHERE 1=0") match {
      case Left(err) =>
        errList = errList :+ SchemaError(JdbcError, err.msg)
      case Right(rs) =>
        try {
          val rsmd = rs.getMetaData
          val ncols = rsmd.getColumnCount
          val fields = new Array[StructField](ncols)
          var i = 0
          while (i < ncols) {
            val columnName = rsmd.getColumnLabel(i + 1)
            val dataType = rsmd.getColumnType(i + 1)
            val typeName = rsmd.getColumnTypeName(i + 1)
            val fieldSize = DecimalType.MAX_PRECISION
            val fieldScale = rsmd.getScale(i + 1)
            val isSigned = rsmd.isSigned(i + 1)
            val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
            val metadata = new MetadataBuilder().putString("name", columnName)


            getCatalystType(dataType, fieldSize, fieldScale, isSigned, typeName) match {

              case Right(columnType) =>
                fields(i) = StructField(columnName, columnType, nullable, metadata.build())
              case Left(err) =>
                errList = errList :+ err
            }
            i = i + 1
          }
          if(errList.isEmpty) {
            schema = Some(new StructType(fields))
          }
        }
        catch {
          case e: Throwable =>
            errList = errList :+ SchemaError(UnexpectedExceptionError, e.getMessage)
        }
        finally {
          rs.close()
        }
    }

    schema match {
      case Some(sch) => Right(sch)
      case None => Left(errList)
    }
  }
}

