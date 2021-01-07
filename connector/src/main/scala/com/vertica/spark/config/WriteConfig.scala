package com.vertica.spark.config

import com.typesafe.scalalogging.Logger
import ch.qos.logback.classic.Level

import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._

trait WriteConfig extends GenericConfig

final case class DistributedFilesystemWriteConfig(override val logLevel: Level) extends WriteConfig {
}

