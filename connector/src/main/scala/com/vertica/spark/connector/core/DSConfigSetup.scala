package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import org.apache.spark.sql.types.StructType


trait DSConfigSetupInterface[T] {
  def init(options : Map[String, String]) // Takes the options map the user passed into the datasource

  def ValidateAndGetConfig() : Either[ConnectorError, T]

  def GetTableSchema() : Either[ConnectorError, StructType]
}
