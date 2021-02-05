package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import com.vertica.spark.config._
import org.apache.spark.sql.catalyst.InternalRow

/**
  * Interface responsible for writing to the Vertica source.
  *
  * This class is initiated and called from each spark worker.
  */
trait DSWriterInterface {
  /**
    * Called before reading to perform any needed setup with the given configuration.
    */
  def openWrite(): Either[ConnectorError, Unit]

  /**
    * Called to write an individual row to the datasource.
    */
  def writeRow(row: InternalRow): Either[ConnectorError, Unit]

  /**
    * Called from the executor to cleanup the individual write operation
    */
  def closeWrite(): Either[ConnectorError, Unit]

  /**
    * Called by the driver to commit all the write results
    */
  def commitRows(): Either[ConnectorError, Unit]
}

class DSWriter(config: WriteConfig, uniqueId: String, pipeFactory: VerticaPipeFactoryInterface = VerticaPipeFactory) extends DSWriterInterface {

  private val pipe = pipeFactory.getWritePipe(config)
  private var blockSize = 0L

  private var data = List[InternalRow]()

  def openWrite(): Either[ConnectorError, Unit] = {
    val sizeOrErr = for{
      size <- pipe.getDataBlockSize
      _ <- pipe.startPartitionWrite(uniqueId)
    } yield (size)

    sizeOrErr match {
      case Left(err) => Left(err)
      case Right(l) =>
        blockSize = l
        Right(())
    }
  }

  def writeRow(row: InternalRow): Either[ConnectorError, Unit] = {
    data = data :+ row
    if(data.length >= blockSize) {
      pipe.writeData(DataBlock(data)) match {
        case Right(_) =>
          data = List[InternalRow]()
          Right(())
        case Left(err) => Left(err)
      }
    }
    else {
      Right(())
    }
  }

  def closeWrite(): Either[ConnectorError, Unit] = {
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

  def commitRows(): Either[ConnectorError, Unit] = ???
}
