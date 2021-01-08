package com.vertica.spark.connector.fs

import com.vertica.spark.datasource.core.DataBlock
import com.vertica.spark.util.error.ConnectorError

trait FileStoreLayerInterface {
  // Write
  def openWriteParquetFile(filename: String) : Either[ConnectorError, Unit]
  def writeDataToParquetFile(filename: String, data: DataBlock) : Either[ConnectorError, Unit]
  def closeWriteParquetFile(filename: String) : Either[ConnectorError, Unit]

  // Read
  def openReadParquetFile(filename: String) : Either[ConnectorError, Unit]
  def readDataFromParquetFile(filename: String, blockSize : Integer) : Either[ConnectorError, DataBlock]
  def closeReadParquetFile(filename: String) : Either[ConnectorError, Unit]

  // Other FS
  def removeFile(filename: String) : Either[ConnectorError, Unit]
  def removeDir(filename: String) : Either[ConnectorError, Unit]
  def createDir(filename: String) : Either[ConnectorError, Unit]
}
