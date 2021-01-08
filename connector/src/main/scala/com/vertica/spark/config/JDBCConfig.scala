package com.vertica.spark.config

import ch.qos.logback.classic.Level

final case class JDBCConfig(host: String, port: Integer, db: String, username: String, val password: String, override val logLevel: Level) extends GenericConfig
