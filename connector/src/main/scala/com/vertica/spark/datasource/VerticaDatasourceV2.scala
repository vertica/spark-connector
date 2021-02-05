package com.vertica.spark.datasource

import com.vertica.spark.datasource.v2._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.expressions.Transform
import java.util

import cats.data.Validated.{Invalid, Valid}
import com.vertica.spark.datasource.core.DSReadConfigSetup
import com.vertica.spark.datasource.core.DSWriteConfigSetup

import collection.JavaConverters._


/**
 * Entry-Point for Spark V2 Datasource.
 *
 * Implements Spark V2 datasource class [[http://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/catalog/TableProvider.html here]]
 *
 * This and the tree of classes returned by is are to be kept light, and hook into the core of the connector
 */
class VerticaSource extends TableProvider {

 /**
  * Used for read operation to get the schema for the table being read from
  *
  * @param caseInsensitiveStringMap A string map of options that was passed in by user to datasource
  * @return The table's schema in spark StructType format
  */
  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap):
      StructType = {

    val table = getTable(schema = null, partitioning = Array.empty[Transform], properties = caseInsensitiveStringMap)
    table.schema()
  }

  /**
  * Gets the structure representing a Vertica table
  *
  * @param schema StructType representing table schema, used for write
  * @param partitioning specified partitioning for the table
  * @param properties A string map of options that was passed in by user to datasource
  * @return [[VerticaTable]]
  */
  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table = {
    new VerticaTable(new CaseInsensitiveStringMap(properties))
  }

}

/**
  * Represents a Vertica table to Spark.
  *
  * Supports Read and Write functionality.
  */
class VerticaTable(caseInsensitiveStringMap: CaseInsensitiveStringMap) extends Table with SupportsRead with SupportsWrite {

  // Cache the scan builder so we don't build it twice
  var scanBuilder : Option[VerticaScanBuilder] = None

/**
  * A name to differentiate this table from other tables
  *
  * @return A string representing a unique name for the table.
  */
  override def name(): String = "VerticaTable" // TODO: change this to db.tablename


/**
  * Should reach out to SQL layer and return schema of the table.
  *
  * @return Spark struct type representing a table schema.
  */
  override def schema(): StructType = this.newScanBuilder(caseInsensitiveStringMap).build().readSchema()

/**
  * Returns a list of capabilities that the table supports.
  *
  * @return Set of [[TableCapability]] representing the functions this source supports.
  */
  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava  // Update this set with any capabilities this table supports

/**
  * Returns a scan builder for reading from Vertica
  *
  * @return [[v2.VerticaScanBuilder]]
  */
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
  {
    this.scanBuilder match {
      case Some(builder) => builder
      case None =>
        val config = (new DSReadConfigSetup).validateAndGetConfig(options.asScala.toMap) match {
          case Invalid(errList) =>
            val errMsgList = for (err <- errList) yield err.msg
            val msg: String = errMsgList.toNonEmptyList.toList.mkString(",\n")
            throw new Exception(msg)
          case Valid(cfg) => cfg
        }
        config.getLogger(classOf[VerticaTable]).debug("Config loaded")

        val scanBuilder = new VerticaScanBuilder(config)
        this.scanBuilder = Some(scanBuilder)
        scanBuilder
    }
  }

/**
  * Returns a write builder for writing to Vertica
  *
  * @return [[v2.VerticaWriteBuilder]]
  */
  def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
  {
    val config = (new DSWriteConfigSetup(schema = Some(info.schema))).validateAndGetConfig(info.options.asScala.toMap) match {
      case Invalid(errList) =>
        val errMsgList = for (err <- errList) yield err.msg
        val msg: String = errMsgList.toNonEmptyList.toList.mkString(",\n")
        throw new Exception(msg)
      case Valid(cfg) => cfg
    }
    config.getLogger(classOf[VerticaTable]).debug("Config loaded")

    new VerticaWriteBuilder(config)
  }
}

