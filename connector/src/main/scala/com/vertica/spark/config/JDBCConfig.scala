package com.vertica.spark.config

import ch.qos.logback.classic.Level

final case class JDBCConfig(host: String, port: Int, db: String, username: String, password: String, override val logLevel: Level) extends GenericConfig
