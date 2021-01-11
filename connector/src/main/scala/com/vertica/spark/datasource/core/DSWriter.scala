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
  def openWrite(config: WriteConfig): Either[ConnectorError, Unit]

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
