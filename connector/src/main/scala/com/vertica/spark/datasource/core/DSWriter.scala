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

/**
  * Interface responsible for writing to the Vertica source.
  *
  * This interface is initiated and called from each spark worker.
  */
trait DSWriterInterface {
  /**
    * Called before reading to perform any needed setup with the given configuration.
    */
  def openWrite(): ConnectorResult[Unit]

  /**
    * Called to write an individual row to the datasource.
    */
  def writeRow(row: InternalRow): ConnectorResult[Unit]

  /**
    * Called from the executor to cleanup the individual write operation
    */
  def closeWrite(): ConnectorResult[Unit]

  /**
    * Called by the driver to commit all the write results
    */
  def commitRows(): ConnectorResult[Unit]
}

object DSWriter {
  def makeDSWriter(pipe: VerticaPipeInterface with VerticaPipeWriteInterface, config: WriteConfig, uniqueId: String): ConnectorResult[DSWriter] = {
    val writer = new DSWriter(pipe, config, uniqueId)
    for {
      _ <- writer.openWrite()
    } yield writer
  }
}

/**
 * Writer class, agnostic to the kind of pipe used for the operation (which VerticaPipe is used)
 *
 * @param config Configuration data definining the write operation.
 * @param uniqueId Unique identifier for this specific writer. The writer for each partition should have a different ID.
 */
class DSWriter private (pipe: VerticaPipeInterface with VerticaPipeWriteInterface, config: WriteConfig, uniqueId: String) extends DSWriterInterface {
  private var blockSize = 0L

  private var data = List[InternalRow]()

  def openWrite(): ConnectorResult[Unit] = {
    for {
      size <- pipe.getDataBlockSize
      _ <- pipe.startPartitionWrite(uniqueId)
      _ = this.blockSize = size
    } yield ()
  }

  def writeRow(row: InternalRow): ConnectorResult[Unit] = {
    data = data :+ row
    if(data.length >= blockSize) {
      pipe.writeData(DataBlock(data)) match {
        case Right(_) =>
          data = List[InternalRow]()
          Right(())
        case Left(errors) => Left(errors)
      }
    }
    else {
      Right(())
    }
  }

  def closeWrite(): ConnectorResult[Unit] = {
    if(data.nonEmpty) {
      for {
        _ <- pipe.writeData(DataBlock(data))
        _ <- pipe.endPartitionWrite()
      } yield ()
    }
    else {
      pipe.endPartitionWrite()
    }
  }

  def commitRows(): ConnectorResult[Unit] = {
    pipe.commit()
  }
}
