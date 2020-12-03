package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import com.vertica.spark.config._
import org.apache.spark.sql.catalyst.InternalRow

trait DSWriterInterface {
  def openWrite(config : WriteConfig) : Either[ConnectorError, Unit]
  def writeRow(row : InternalRow) : Either[ConnectorError, Unit]
  def commitRows() : Either[ConnectorError, Unit]
  def closeWrite() : Either[ConnectorError, Unit]
}
