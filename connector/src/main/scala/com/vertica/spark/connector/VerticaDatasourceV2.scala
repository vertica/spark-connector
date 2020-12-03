package com.vertica.spark.datasource

import com.vertica.spark.datasource.v2._

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.expressions.Transform

import java.util
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
                        map: util.Map[String, String]): Table =
    new VerticaTable()

}

class VerticaTable extends Table with SupportsRead with SupportsWrite {
  override def name(): String = "VerticaTable"

  // Should reach to SQL layer and return schema of the table
  // For now just a list of strings
  override def schema(): StructType = new StructType()

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava  // Update this set with any capabilities this table supports

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
  {
    new VerticaScanBuilder()
  }

  def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
  {
    new VerticaWriteBuilder()
  }

}

