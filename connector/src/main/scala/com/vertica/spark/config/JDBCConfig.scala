package com.vertica.spark.config

import ch.qos.logback.classic.Level

final case class JDBCConfig(val host: String, val port: Integer, val db: String, val username: String, val password: String, override val logLevel: Level) extends GenericConfig
