package com.vertica.spark.config

import ch.qos.logback.classic.Level

final case class FileStoreConfig(address: String, override val logLevel: Level) extends GenericConfig
