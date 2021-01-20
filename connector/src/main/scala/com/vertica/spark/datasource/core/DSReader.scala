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
  val logger: Logger = config.getLogger(classOf[DSReader])

  val pipe = pipeFactory.getReadPipe(config)

  var block: Option[DataBlock] = None
  var i: Int = 0

  def openRead(): Either[ConnectorError, Unit] = {
    partition match {
      case verticaPartition: VerticaPartition => pipe.startPartitionRead(verticaPartition)
      case _ =>
        logger.error("Unexpected state: partition of type 'VerticaPartition' was expected but not received")
        Left(ConnectorError(InvalidPartition))
    }
  }

  def readRow(): Either[ConnectorError, Option[InternalRow]] = {
    // Get current or next data block
    val dataBlock: DataBlock = block match {
      case None =>
        pipe.readData match {
          case Left(err) =>
            err.err match {
              case DoneReading => return Right(None) // If there's no more data to be read, return nothing for row
              case _ => return Left(err) // If something else went wrong, return error
            }
          case Right(data) =>
            block = Some(data)
            data
        }
      case Some(block) => block
    }

    // Get row from block. If this is the last row of the block, reset the block
    val row = dataBlock.data(i)
    i += 1
    if(i >= dataBlock.data.size) {
      block = None
      i = 0
    }

    Right(Some(row))
  }

  def closeRead(): Either[ConnectorError, Unit] = {
    pipe.endPartitionRead()
  }
}