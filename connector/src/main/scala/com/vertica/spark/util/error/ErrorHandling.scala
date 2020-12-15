package com.vertica.spark.util.error

/**
  * Enumeration of the list of possible connector errors.
  */
object ConnectorErrorType extends Enumeration {
  type ConnectorErrorType = Value

  val LOGGING_LEVEL_PARSE_ERR = Value("logging_level is incorrect. Use ERROR, INFO, DEBUG, or WARNING instead.")
}
import ConnectorErrorType._


/**
  * General connector error returned when something goes wrong.
  */
case class ConnectorError(err : ConnectorErrorType) {
  def msg = err.toString
}

/**
  * Specific jdbc connector error returned when an operation with the JDBC interface goes wrong.
  */
case class JDBCLayerError(val msg: String)
