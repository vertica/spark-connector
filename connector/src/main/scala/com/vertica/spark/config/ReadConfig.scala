package com.vertica.spark.config

import ch.qos.logback.classic.Level

trait ReadConfig extends GenericConfig

final case class DistributedFilesystemReadConfig(override val logLevel: Level, jdbcConfig: JDBCConfig, fileStoreConfig: FileStoreConfig, tablename: String, partitionCount: Option[Int], metadata: Option[VerticaMetadata]) extends ReadConfig
