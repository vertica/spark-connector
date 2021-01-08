package com.vertica.spark.connector.fs

import com.vertica.spark.datasource.core.DataBlock
import com.vertica.spark.util.error.ConnectorError

trait FileStoreLayerInterface {
  // Write
  def OpenWriteParquetFile(filename : String) : Either[ConnectorError, Unit]
  def WriteDataToParquetFile(filename : String, data : DataBlock) : Either[ConnectorError, Unit]
  def CloseWriteParquetFile(filename : String) : Either[ConnectorError, Unit]

  // Read
  def OpenReadParquetFile(filename : String) : Either[ConnectorError, Unit]
  def ReadDataFromParquetFile(filename : String, blockSize : Integer) : Either[ConnectorError, DataBlock]
  def CloseReadParquetFile(filename : String) : Either[ConnectorError, Unit]

  // Other FS
  def RemoveFile(filename : String) : Either[ConnectorError, Unit]
  def RemoveDir(filename : String) : Either[ConnectorError, Unit]
  def CreateDir(filename : String) : Either[ConnectorError, Unit]
}
