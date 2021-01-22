package com.vertica.spark.datasource.v2

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.InternalRow

import collection.JavaConverters._
import com.vertica.spark.config.ReadConfig
import cats.data.Validated.{Invalid, Valid}
import com.vertica.spark.config.DistributedFilesystemReadConfig
import com.vertica.spark.datasource.VerticaTable
import com.vertica.spark.datasource.core.{DSReadConfigSetup, DSReader, VerticaPartition}
import com.vertica.spark.util.error.ConnectorError
import com.vertica.spark.util.error.ConnectorErrorType.PartitioningError

/**
  * Builds the scan class for use in reading of Vertica
  */
class VerticaScanBuilder(options: CaseInsensitiveStringMap) extends ScanBuilder {
/**
  * Builds the class representing a scan of a Vertica table
  *
  * @return [[VerticaScan]]
  */
  override def build(): Scan = {

    val config = (new DSReadConfigSetup).validateAndGetConfig(this.options.asScala.toMap) match {
      case Invalid(errList) =>
        val errMsgList = for (err <- errList) yield err.msg
        val msg: String = errMsgList.toNonEmptyList.toList.mkString(",\n")
        throw new Exception(msg)
      case Valid(cfg) => cfg
    }

    config.getLogger(classOf[VerticaTable]).debug("Config loaded")
    new VerticaScan(config)
  }
}


/**
  * Represents a scan of a Vertica table.
  *
  * Extends mixin class to represent type of read. Options are Batch or Stream, we are doing a batch read.
  */
class VerticaScan(config: ReadConfig) extends Scan with Batch {



  /**
  * Schema of scan (can be different than full table schema)
  */
  override def readSchema(): StructType = {
    (new DSReadConfigSetup).getTableSchema(config) match {
      case Right(schema) => schema
      case Left(err) => throw new Exception(err.msg)
    }
  }

/**
  * Returns this object as an instance of the Batch interface
  */
  override def toBatch(): Batch = this


/**
  * Returns an array of partitions. These contain the information necesary for each reader to read it's portion of the data
  */
  override def planInputPartitions(): Array[InputPartition] = {
    (new DSReadConfigSetup).performInitialSetup(config) match {
      case Left(err) => throw new Exception(err.msg)
      case Right(opt) => opt match  {
        case None =>
          val err = ConnectorError(PartitioningError)
          throw new Exception(err.msg)
        case Some(partitionInfo) => partitionInfo.partitionSeq
      }
    }
  }


/**
  * Creates the reader factory which will be serialized and sent to workers
  *
  * @return [[VerticaReaderFactory]]
  */
  override def createReaderFactory(): PartitionReaderFactory = new VerticaReaderFactory(config) }

/**
  * Factory class for creating the Vertica reader
  *
  * This class is seriazlized and sent to each worker node. On the worker, createReader will be called with the given partition of data for that worker.
  */
class VerticaReaderFactory(config: ReadConfig) extends PartitionReaderFactory {
/**
  * Called from the worker node to get the reader for that node
  *
  * @return [[VerticaBatchReader]]
  */
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
  {
    new VerticaBatchReader(config, partition)
  }

}

/**
  * Reader class that reads rows from the underlying datasource
  */
class VerticaBatchReader(config: ReadConfig, partition: InputPartition) extends PartitionReader[InternalRow] {

  val reader = new DSReader(config, partition)

  // Open the read
  reader.openRead()

  var row: Option[InternalRow] = None

/**
  * Returns true if there are more rows to read
  */
  override def next: Boolean =
  {
    reader.readRow() match {
      case Left(err) =>
        throw new Exception(err.msg)
      case Right(r) =>
        row = r
    }
    row match {
      case Some(_) => true
      case None => false
    }
  }

/**
  * Return the current row
  */
  override def get: InternalRow = {
    val r = row.get
    println("API ROW: " + r)
    r
  }

/**
  * Calls underlying datasource to do any needed cleanup
  */
  def close(): Unit = reader.closeRead()
}
