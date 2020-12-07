package com.vertica.spark.util.error

case class ConnectorError(val msg: String)

case class JDBCLayerError(val msg: String)
