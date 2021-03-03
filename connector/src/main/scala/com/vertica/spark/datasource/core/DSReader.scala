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

import com.typesafe.scalalogging.Logger
import com.vertica.spark.util.error._
import com.vertica.spark.config._
import com.vertica.spark.util.error.ConnectorErrorType.{DoneReading, InvalidPartition}
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
  def readRow(): Either[ConnectorError, Option[InternalRow]]

  /**
   * Called when all reading is done, to perform any needed cleanup operations.
   *
   * Returns None for row if this is there are no more rows to read.
   */
  def closeRead(): Either[ConnectorError, Unit]
}


class DSReader(config: ReadConfig, partition: InputPartition, pipeFactory: VerticaPipeFactoryInterface = VerticaPipeFactory) extends DSReaderInterface {
  private val logger: Logger = config.getLogger(classOf[DSReader])

  private val pipe = pipeFactory.getReadPipe(config)

  private var block: Option[DataBlock] = None
  private var i: Int = 0

  def openRead(): Either[ConnectorError, Unit] = {
    i = 0
    partition match {
      case verticaPartition: VerticaPartition => pipe.startPartitionRead(verticaPartition)
      case _ =>
        logger.error("Unexpected state: partition of type 'VerticaPartition' was expected but not received")
        Left(ConnectorError(InvalidPartition))
    }
  }

  def readRow(): Either[ConnectorError, Option[InternalRow]] = {
    val ret = for {
      // Get current or next data block
      dataBlock <- block match {
        case None =>
          pipe.readData match {
            case Left(err) => Left(err)
            case Right(data) =>
              block = Some(data)
              Right(data)
          }
        case Some(block) => Right(block)
      }

      // Get row from block. If this is the last row of the block, reset the block
      _ <- if (dataBlock.data.isEmpty) {
        Left(ConnectorError(DoneReading))
      } else {
        Right(())
      }

      row = dataBlock.data(i)
      _ = i += 1
      _ = if (i >= dataBlock.data.size) {
        block = None
        i = 0
      } else {
        ()
      }
    } yield Some(row)

    ret match {
      case Right(optRow) => Right(optRow)
      case Left(err) => err.err match {
        case DoneReading => Right(None) // If there's no more data to be read, return nothing for row
        case _ => Left(err) // If something else went wrong, return error
      }
    }
  }

  def closeRead(): Either[ConnectorError, Unit] = {
    pipe.endPartitionRead()
  }
}
