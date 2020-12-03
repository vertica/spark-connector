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

class VerticaScanBuilder extends ScanBuilder {
  override def build(): Scan = new VerticaScan()
}

class VerticaPartition extends InputPartition

// Options are Batch or Stream, we are doing a batch read
class VerticaScan extends Scan with Batch {
  // Schema of scan (can be different than full table schema)
  override def readSchema(): StructType = StructType(Array(StructField("a", IntegerType), StructField("b", FloatType)))

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] = Array(new VerticaPartition())

  override def createReaderFactory(): PartitionReaderFactory = new VerticaReaderFactory()
}

class VerticaReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition) : PartitionReader[InternalRow] =
  {
    new VerticaBatchReader()
  }

}

class VerticaBatchReader extends PartitionReader[InternalRow] {
  // hardcoded vertica details for now
  def next =
  {
    false
  }

  def get = {
    val v1 : Integer = 1
    val v2 : Float = 2
    val row = InternalRow(v1, v2)
    row
  }

  def close() = Unit
}
