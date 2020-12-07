package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import com.vertica.spark.config._
import org.apache.spark.sql.catalyst.InternalRow

case class DataBlock(data : List[InternalRow])

case class VerticaMetadata()

trait VerticaPipeInterface {
  // Config
  def getMetadata() : Either[ConnectorError, VerticaMetadata] // includes schema and things like node information / segmentation -- should have caching mechanism
  def getDataBlockSize() : Either[ConnectorError, Long] // should return the default number of rows to read/write from this pipe at a time.
}

trait VerticaPipeWriteInterface { // Mixin
  def doPreWriteSteps() : Either[ConnectorError, Unit]   // Called from driver
  def startPartitionWrite() : Either[ConnectorError, Unit] // Called from executor
  def writeData(data : DataBlock) : Either[ConnectorError, Unit]  // Called from executor
  def endPartitionWrite() : Either[ConnectorError, Unit]   // Called from executor
  def commit() : Either[ConnectorError, Unit]              // Called from driver
}

trait VerticaPipeReadInterface { // Mixin
  def doPreReadSteps() : Either[ConnectorError, Unit]   // Called from driver
  def startPartitionRead() : Either[ConnectorError, Unit]  // Called from executor
  def readData : Either[ConnectorError, DataBlock]         // Called from executor
  def endPartitionRead() : Either[ConnectorError, Unit]   // Called from executor
}
