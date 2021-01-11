package com.vertica.spark.config

import ch.qos.logback.classic.Level

trait WriteConfig extends GenericConfig

final case class DistributedFilesystemWriteConfig(override val logLevel: Level) extends WriteConfig {
}

