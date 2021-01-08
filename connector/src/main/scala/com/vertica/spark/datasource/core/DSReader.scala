package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import com.vertica.spark.config._
import org.apache.spark.sql.catalyst.InternalRow

/**
  * Interface responsible for reading from the Vertica source.
  *
  * This class is initiated and called from each spark worker.
  */
trait DSReaderInterface {
/**
  * Called before reading to perform any needed setup with the given configuration.
  */
  def openRead(config: ReadConfig): Either[ConnectorError, Unit]

/**
  * Called by spark to read an individual row
  */
  def readRow(): Either[ConnectorError, Option[InternalRow]]

/**
  * Called when all reading is done, to perform any needed cleanup operations.
  */
  def closeRead(): Either[ConnectorError, Unit]
}
