package com.vertica.spark.util.error

/**
  * General connector error returned when something goes wrong.
  */
case class ConnectorError(val msg: String)

/**
  * Specific jdbc connector error returned when an operation with the JDBC interface goes wrong.
  */
case class JDBCLayerError(val msg: String)
