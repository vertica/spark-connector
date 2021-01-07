package com.vertica.spark.config

import com.typesafe.scalalogging.Logger
import ch.qos.logback.classic.Level

trait ReadConfig extends GenericConfig

final case class DistributedFilesystemReadConfig(override val logLevel: Level, val jdbcConfig : JDBCConfig, val tablename: String, val metadata: Option[VerticaMetadata]) extends ReadConfig
