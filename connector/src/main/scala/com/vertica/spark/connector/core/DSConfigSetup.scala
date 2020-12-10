package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import org.apache.spark.sql.types.StructType


/**
  * Interface for taking input of user selected options, performing any setup steps required, and returning the proper configuration structure for the operation.
  */
trait DSConfigSetupInterface[T] {
/**
  * Validates and returns the configuration structure for the specific read/write operation.
  *
  * Will return a error if validation of the user options failed, otherwise will return the configuration structure expected by the writer/reader.
  */
  def ValidateAndGetConfig() : Either[ConnectorError, T]

/**
  * Returns the schema for the table as required by Spark.
  */
  def GetTableSchema() : Either[ConnectorError, StructType]
}
