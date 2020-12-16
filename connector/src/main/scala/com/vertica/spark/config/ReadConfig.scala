package com.vertica.spark.config

import com.typesafe.scalalogging.Logger
import ch.qos.logback.classic.Level
import ch.qos.logback.classic

trait ReadConfig extends GenericConfig

case class DistributedFilestoreReadConfig(override val logLevel: Level) extends ReadConfig {
}
