package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import com.vertica.spark.config._
import org.apache.spark.sql.catalyst.InternalRow

trait DSReaderInterface {
  def openRead(config : ReadConfig) : Either[ConnectorError, Unit]
  def readRow() : Either[ConnectorError, Option[InternalRow]]
  def closeRead() : Either[ConnectorError, Unit]
}
