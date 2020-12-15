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
    val config = dsConfigSetup.validateAndGetConfig() match
    {
      case Left(err) => throw new Exception(err.msg)
      case Right(cfg) => cfg.asInstanceOf[DistributedFilestoreReadConfig]
    }

    config.GetLogger(classOf[VerticaTable]).debug("Config loaded")
    new VerticaScanBuilder()
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
  {
    val dsConfigSetup: DSConfigSetupInterface[WriteConfig] = new DSWriteConfigSetup(configOptions)
    new VerticaWriteBuilder()
  }
}

