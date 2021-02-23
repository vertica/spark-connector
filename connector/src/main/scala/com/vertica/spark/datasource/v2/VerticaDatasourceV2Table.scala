package com.vertica.spark.datasource.v2

import java.util

import cats.data.Validated.{Invalid, Valid}
import com.vertica.spark.datasource.core.{DSReadConfigSetup, DSWriteConfigSetup}
import com.vertica.spark.datasource.v2
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import collection.JavaConverters._

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
  override def schema(): StructType = {
    // Check if there's a valid read config with schema for the table, if not return empty schema
    (new DSReadConfigSetup).validateAndGetConfig(caseInsensitiveStringMap.asScala.toMap) match {
      case Invalid(_) => new StructType()
      case Valid(_) => this.newScanBuilder(caseInsensitiveStringMap).build().readSchema()
    }

  }

  /**
   * Returns a list of capabilities that the table supports.
   *
   * @return Set of [[TableCapability]] representing the functions this source supports.
   */
  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.TRUNCATE, TableCapability.ACCEPT_ANY_SCHEMA).asJava  // Update this set with any capabilities this table supports

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
    val config = new DSWriteConfigSetup(schema = Some(info.schema)).validateAndGetConfig(info.options.asScala.toMap) match {
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

