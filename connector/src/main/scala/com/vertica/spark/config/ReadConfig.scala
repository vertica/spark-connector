package com.vertica.spark.config

import ch.qos.logback.classic.Level

trait ReadConfig extends GenericConfig

final case class DistributedFilesystemReadConfig(override val logLevel: Level, jdbcConfig: JDBCConfig, tablename: String, metadata: Option[VerticaMetadata]) extends ReadConfig
