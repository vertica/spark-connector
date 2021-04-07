// (c) Copyright [2020-2021] Micro Focus or one of its affiliates.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.vertica.spark.datasource.core

import com.vertica.spark.config._
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition

final case class DataBlock(data: List[InternalRow])

/**
 * Represents a partition of the data being read
 *
 * One spark worker is created per partition. The number of these partitions is decided at the driver.
 */
class VerticaPartition extends InputPartition

/**
 * Partitioning information.
 * @param partitionSeq Sequence of InputPartitions, where each InputPartition contains info for reading one partition of the data.
 */
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
  def getMetadata: ConnectorResult[VerticaMetadata]

  /**
    * Returns the default number of rows to read/write from this pipe at a time.
    */
  def getDataBlockSize: ConnectorResult[Long]
}

/**
 * Mixin for [[VerticaPipeInterface]] for writing to Vertica
 */
trait VerticaPipeWriteInterface {
  /**
    * Initial setup for the whole write operation. Called by driver.
    */
  def doPreWriteSteps(): ConnectorResult[Unit]

  /**
   * Initial setup for the write of an individual partition. Called by executor.
   *
   * @param uniqueId Unique identifier for the partition being written
   */
  def startPartitionWrite(uniqueId: String): ConnectorResult[Unit]

  /**
    * Write a block of data to the underlying source. Called by executor.
    */
  def writeData(data: DataBlock): ConnectorResult[Unit]

  /**
    * Ends the write, doing any necessary cleanup. Called by executor once writing of the given partition is done.
    */
  def endPartitionWrite(): ConnectorResult[Unit]

  /**
    * Commits the data being written. Called by the driver once all executors have succeeded writing.
    */
  def commit(): ConnectorResult[Unit]
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
  def doPreReadSteps(): ConnectorResult[PartitionInfo]

  /**
    * Initial setup for the read of an individual partition. Called by executor.
    */
  def startPartitionRead(partition: VerticaPartition): ConnectorResult[Unit]


  /**
    * Reads a block of data to the underlying source. Called by executor.
    */
  def readData: ConnectorResult[DataBlock]


  /**
    * Ends the read, doing any necessary cleanup. Called by executor once reading the partition is done.
    */
  def endPartitionRead(): ConnectorResult[Unit]
}
