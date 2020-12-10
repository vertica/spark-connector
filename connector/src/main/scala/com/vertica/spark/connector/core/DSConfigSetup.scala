package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import org.apache.spark.sql.types.StructType


trait DSConfigSetupInterface[T] {
  def validateAndGetConfig() : Either[ConnectorError, T]

  def getTableSchema() : Either[ConnectorError, StructType]
}
