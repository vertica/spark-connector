package com.vertica.spark.datasource

import com.vertica.spark.datasource.v2._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.expressions.Transform
import java.util

import com.vertica.spark.config.{DistributedFilestoreReadConfig, ReadConfig, WriteConfig}
import com.vertica.spark.util.error.ConnectorError

import com.vertica.spark.datasource.core.DSConfigSetupInterface
import com.vertica.spark.datasource.core.DSReadConfigSetup
import com.vertica.spark.datasource.core.DSWriteConfigSetup

import collection.JavaConverters._
import com.vertica.spark.config._


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
      StructType =
        getTable(null, Array.empty[Transform], caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

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
    new VerticaTable(properties.asScala.toMap)
  }

}

/**
  * Represents a Vertica table to Spark.
  *
  * Supports Read and Write functionality.
  */
class VerticaTable(val configOptions: Map[String, String]) extends Table with SupportsRead with SupportsWrite {
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
  override def schema(): StructType = new StructType()

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
    val dsConfigSetup: DSConfigSetupInterface[ReadConfig] = new DSReadConfigSetup(configOptions)
    val config = dsConfigSetup.validateAndGetConfig() match
    {
      case Left(err) => throw new Exception(err.msg)
      case Right(cfg) => cfg.asInstanceOf[DistributedFilestoreReadConfig]
    }

    config.GetLogger(classOf[VerticaTable]).debug("Config loaded")

    // TODO: Use config for scan builder
    new VerticaScanBuilder()
  }

/**
  * Returns a write builder for writing to Vertica
  *
  * @return [[v2.VerticaWriteBuilder]]
  */
  def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
  {
    val dsConfigSetup: DSConfigSetupInterface[WriteConfig] = new DSWriteConfigSetup(configOptions)

    // TODO: Use config for write builder
    new VerticaWriteBuilder()
  }
}

