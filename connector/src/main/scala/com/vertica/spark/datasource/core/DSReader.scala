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

import com.vertica.spark.util.error._
import com.vertica.spark.config._
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition

/**
  * Interface responsible for reading from the Vertica source.
  *
  * This class is initiated and called from each spark worker.
  */
trait DSReaderInterface {
  /**
    * Called by spark to read an individual row
    */
  def readRow(): ConnectorResult[Option[InternalRow]]

  /**
   * Called when all reading is done, to perform any needed cleanup operations.
   *
   * Returns None for row if this is there are no more rows to read.
   */
  def closeRead(): ConnectorResult[Unit]
}


class DSReader(config: ReadConfig, partition: InputPartition, pipeFactory: VerticaPipeFactoryInterface = VerticaPipeFactory) extends DSReaderInterface {
  private val pipe = pipeFactory.getReadPipe(config)

  private var currentBlock: Option[DataBlock] = None

  def openRead(): ConnectorResult[Unit] = {
    partition match {
      case verticaPartition: VerticaPartition => pipe.startPartitionRead(verticaPartition)
      case _ => Left(InvalidPartition().context(
          "Unexpected state: partition of type 'VerticaPartition' was expected but not received"))
    }
  }

  def tryGetDataBlock(currentDataBlock: Option[DataBlock]): ConnectorResult[Option[DataBlock]] = {
    currentDataBlock match {
      case None => this.pipe.readData
      case Some(block) => Right(Some(block))
    }
  }

  def readRow(): ConnectorResult[Option[InternalRow]] = {
    for {
      // Get current or next data block
      dataBlock <- this.tryGetDataBlock(this.currentBlock)
      optRow = dataBlock.flatMap(_.next())
      _ = optRow match {
        case Some(_) => ()
        case None => this.currentBlock = None
      }
    } yield optRow
  }

  def closeRead(): ConnectorResult[Unit] = {
    pipe.endPartitionRead()
  }
}
