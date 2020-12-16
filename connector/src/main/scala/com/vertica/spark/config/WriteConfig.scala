package com.vertica.spark.config

import com.typesafe.scalalogging.Logger
import ch.qos.logback.classic.Level
import ch.qos.logback.classic

trait WriteConfig extends GenericConfig

case class DistributedFilestoreWriteConfig(override val logLevel: Level) extends WriteConfig {
}

