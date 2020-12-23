package com.vertica.spark.util.error

/**
  * Enumeration of the list of possible connector errors.
  */
object ConnectorErrorType extends Enumeration {
  type ConnectorErrorType = Value

  val InvalidLoggingLevel = Value("logging_level is incorrect. Use ERROR, INFO, DEBUG, or WARNING instead.")
}
import ConnectorErrorType._


/**
  * General connector error returned when something goes wrong.
  */
case class ConnectorError(err: ConnectorErrorType) {
  def msg = err.toString
}

/**
  * Enumeration of the list of possible connector errors.
  */
object JdbcErrorType extends Enumeration {
  type JdbcErrorType = Value

  val ConnectionError = Value("Connection to the JDBC source is down or invalid")
  val DataTypeError = Value("Wrong data type")
  val SyntaxError = Value("Syntax error")
  val GenericError = Value("JDBC error")
}
import JdbcErrorType._



/**
  * Specific jdbc connector error returned when an operation with the JDBC interface goes wrong.
  */
case class JDBCLayerError(err: JdbcErrorType, value: String = "") {
  def msg = {
    err match {
      case SyntaxError => err.toString + ": " + value
      case DataTypeError => err.toString + ": " + value
      case GenericError => err.toString + ": " + value
      case _ => err.toString
    }
  }
}
