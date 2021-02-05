package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import com.vertica.spark.config._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition

final case class DataBlock(data: List[InternalRow])

/**
 * Represents a partition of the data being read
 *
 * One spark worker is created per partition. The number of these partitions is decided at the driver.
 */
class VerticaPartition extends InputPartition

final case class PartitionInfo(partitionSeq: Array[InputPartition])

/**
  * Interface for the pipe that connects us to Vertica. Agnostic to the method used to transfer the data.
  *
  * Mixins: [[VerticaPipeWriteInterface]], [[VerticaPipeWriteInterface]]
  */
trait VerticaPipeInterface {
  /**
    * Retrieve any needed metadata for a table needed to inform the configuration of the operation.
    *
    * Can include schema and things like node information / segmentation -- should have caching mechanism
    */
  def getMetadata: Either[ConnectorError, VerticaMetadata]

  /**
    * Returns the default number of rows to read/write from this pipe at a time.
    */
  def getDataBlockSize: Either[ConnectorError, Long]
}

/**
 * Mixin for [[VerticaPipeInterface]] for writing to Vertica
 */
trait VerticaPipeWriteInterface {
  /**
    * Initial setup for the whole write operation. Called by driver.
    */
  def doPreWriteSteps(): Either[ConnectorError, Unit]

  /**
   * Initial setup for the write of an individual partition. Called by executor.
   *
   * @param uniqueId Unique identifier for the partition being written
   */
  def startPartitionWrite(uniqueId: String): Either[ConnectorError, Unit]

  /**
    * Write a block of data to the underlying source. Called by executor.
    */
  def writeData(data: DataBlock): Either[ConnectorError, Unit]

  /**
    * Ends the write, doing any necessary cleanup. Called by executor once writing of the given partition is done.
    */
  def endPartitionWrite(): Either[ConnectorError, Unit]

  /**
    * Commits the data being written. Called by the driver once all executors have succeeded writing.
    */
  def commit(): Either[ConnectorError, Unit]
}

/**
 * Mixin for [[VerticaPipeInterface]] for reading from Vertica
 */
trait VerticaPipeReadInterface {

  /**
   * Initial setup for the whole read operation. Called by driver.
   *
   * @return Partitioning information for how the read operation will be partitioned across spark nodes
   */
  def doPreReadSteps(): Either[ConnectorError, PartitionInfo]

  /**
    * Initial setup for the read of an individual partition. Called by executor.
    */
  def startPartitionRead(partition: VerticaPartition): Either[ConnectorError, Unit]


  /**
    * Reads a block of data to the underlying source. Called by executor.
    */
  def readData: Either[ConnectorError, DataBlock]


  /**
    * Ends the read, doing any necessary cleanup. Called by executor once reading the partition is done.
    */
  def endPartitionRead(): Either[ConnectorError, Unit]
}
