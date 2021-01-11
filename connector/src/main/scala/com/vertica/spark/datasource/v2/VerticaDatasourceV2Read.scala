package com.vertica.spark.datasource.v2

import java.util

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.expressions.Transform

import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

import collection.JavaConverters._
import java.sql.DriverManager
import java.sql.Connection

/**
  * Builds the scan class for use in reading of Vertica
  */
class VerticaScanBuilder extends ScanBuilder {
/**
  * Builds the class representing a scan of a Vertica table
  *
  * @return [[VerticaScan]]
  */
  override def build(): Scan = new VerticaScan()
}

/**
  * Represents a partition of the data being read
  *
  * One spark worker is created per partition. The number of these partitions is decided at the driver.
  */
class VerticaPartition extends InputPartition

/**
  * Represents a scan of a Vertica table.
  *
  * Extends mixin class to represent type of read. Options are Batch or Stream, we are doing a batch read.
  */
class VerticaScan extends Scan with Batch {
/**
  * Schema of scan (can be different than full table schema)
  */
  override def readSchema(): StructType = StructType(Array(StructField("a", IntegerType), StructField("b", FloatType)))

/**
  * Returns this object as an instance of the Batch interface
  */
  override def toBatch(): Batch = this


/**
  * Returns an array of partitions. These contain the information necesary for each reader to read it's portion of the data
  */
  override def planInputPartitions(): Array[InputPartition] = Array(new VerticaPartition())

/**
  * Creates the reader factory which will be serialized and sent to workers
  *
  * @return [[VerticaReaderFactory]]
  */
  override def createReaderFactory(): PartitionReaderFactory = new VerticaReaderFactory()
}

/**
  * Factory class for creating the Vertica reader
  *
  * This class is seriazlized and sent to each worker node. On the worker, createReader will be called with the given partition of data for that worker.
  */
class VerticaReaderFactory extends PartitionReaderFactory {
/**
  * Called from the worker node to get the reader for that node
  *
  * @return [[VerticaBatchReader]]
  */
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
  {
    new VerticaBatchReader()
  }

}

/**
  * Reader class that reads rows from the underlying datasource
  */
class VerticaBatchReader extends PartitionReader[InternalRow] {
  // hardcoded vertica details for now

/**
  * Returns true if there are more rows to read
  */
  override def next: Boolean =
  {
    false
  }

/**
  * Return the current row
  */
  override def get: InternalRow = {
    val v1: Int = 1
    val v2: Float = 2
    val row = InternalRow(v1, v2)
    row
  }

/**
  * Calls underlying datasource to do any needed cleanup
  */
  def close(): Unit = Unit
}
