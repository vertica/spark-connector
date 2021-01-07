package com.vertica.spark.util.error

/**
  * Enumeration of the list of possible connector errors.
  */
object ConnectorErrorType extends Enumeration {
  type ConnectorErrorType = Value

  val InvalidLoggingLevel = Value("logging_level is incorrect. Use ERROR, INFO, DEBUG, or WARNING instead.")
  val ConfigBuilderError = Value("There was an unexpected problem building the configuration object. Mandatory value missing.")
  val HostMissingError = Value("The 'host' param is missing. Please specify the IP address or hostname of the Vertica server to connect to.")
  val DbMissingError = Value("The 'db' param is missing. Please specify the name of the Vertica database to connect to..")
  val UserMissingError = Value("The 'user' param is missing. Please specify the username to use for authenticating with Vertica.")
  val PasswordMissingError = Value("The 'user' param is missing. Please specify the username to use for authenticating with Vertica.")
  val TablenameMissingError = Value("The 'tablename' param is missing. Please specify the name of the table to use for authenticating with Vertica.")
  val InvalidPortError = Value("The 'port' param specified is invalid. Please specify a valid integer between 1 and 65535.")
  val SchemaDiscoveryError = Value("Failed to discover the schema of the table. There may be an issue with connectivity to the database")
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
      case SyntaxError | DataTypeError | GenericError => err.toString + ": " + value
      case _ => err.toString
    }
  }
}


/**
  * Enumeration of the list of possible connector errors.
  */
object SchemaErrorType extends Enumeration {
  type SchemaErrorType = Value

  val MissingConversionError = Value("Could not find conversion for unsupported SQL type")
  val UnexpectedExceptionError = Value("Unexpected exception while retrieving schema: ")
  val JdbcError = Value("JDBC failure when trying to retrieve schema")
}
import SchemaErrorType._



/**
  * Specific jdbc connector error returned when an operation with the JDBC interface goes wrong.
  */
case class SchemaError(err: SchemaErrorType, value: String = "") {
  def msg = {
    err match {
      case MissingConversionError | UnexpectedExceptionError => err.toString + ": " + value
      case JdbcError => err.toString + ", JDBC Error: \n " + value
      case _ => err.toString
    }
  }
}
