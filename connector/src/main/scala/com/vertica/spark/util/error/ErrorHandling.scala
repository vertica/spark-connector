package com.vertica.spark.util.error

object ConnectorErrorType extends Enumeration {
  type ConnectorErrorType = Value

  val LOGGING_LEVEL_PARSE_ERR = Value("logging_level is incorrect. Use ERROR, INFO, DEBUG, or WARNING instead.")
}
import ConnectorErrorType._

case class ConnectorError(err : ConnectorErrorType) {
  def msg = err.toString
}

case class JDBCLayerError(val msg: String)
