package com.vertica.spark.datasource

import com.vertica.spark.datasource.v2._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.expressions.Transform
import java.util

import com.typesafe.scalalogging.Logger
import ch.qos.logback.classic
import ch.qos.logback.classic.Level
import com.vertica.spark.config.{HDFSMethodReadConfig, ReadConfig, WriteConfig}
import com.vertica.spark.datasource.core.DSConfigSetupInterface
import com.vertica.spark.util.error.ConnectorError

import collection.JavaConverters._


/**
  Implementation of Spark V2 Datasource.
  Kept light, hooks into the core of the connector
  **/

class VerticaSource extends TableProvider {

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap):
      StructType =
        getTable(null, Array.empty[Transform], caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  override def getTable(structType: StructType,
                        transforms: Array[Transform],
                        map: util.Map[String, String]): Table = {
    new VerticaTable(map.asScala.toMap)
  }

}

class VerticaTable(val configOptions: Map[String, String]) extends Table with SupportsRead with SupportsWrite {
  override def name(): String = "VerticaTable"

  // Should reach to SQL layer and return schema of the table
  // For now just a list of strings
  override def schema(): StructType = new StructType()

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava  // Update this set with any capabilities this table supports

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
  {
    val dsConfigSetup: DSConfigSetupInterface[ReadConfig] = new DSReadConfigSetup(configOptions)
    new VerticaScanBuilder()
  }

  def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
  {
    val dsConfigSetup: DSConfigSetupInterface[WriteConfig] = new DSWriteConfigSetup(configOptions)
    new VerticaWriteBuilder()
  }

}

class DSReadConfigSetup(val config: Map[String, String]) extends DSConfigSetupInterface[ReadConfig] {
  override def validateAndGetConfig(): Either[ConnectorError, ReadConfig] = {
    val logLevel: Option[Level] = this.config.get("logging_level").map {
      case "ERROR" => Some(Level.ERROR)
      case "DEBUG" => Some(Level.DEBUG)
      case "WARNING" => Some(Level.WARN)
      case "INFO" => Some(Level.INFO)
      case _ => None
    }.getOrElse(Some(Level.ERROR))

    logLevel match {
      case Some(level) =>
        val logger = Logger("logback")
        logger.underlying.asInstanceOf[classic.Logger].setLevel(level)

        // Always return HDFSMethodReadConfig since we don't support direct yet
        Right(HDFSMethodReadConfig(logger))
      case None => Left(ConnectorError("logging_level is incorrect. Use ERROR, INFO, DEBUG, or WARNING instead."))
    }
  }

  override def getTableSchema(): Either[ConnectorError, StructType] = ???
}

class DSWriteConfigSetup(val config: Map[String, String]) extends DSConfigSetupInterface[WriteConfig] {
  override def validateAndGetConfig(): Either[ConnectorError, WriteConfig] = ???

  override def getTableSchema(): Either[ConnectorError, StructType] = ???
}
