package com.vertica.spark.config

import com.typesafe.scalalogging.Logger
import ch.qos.logback.classic.Level
import ch.qos.logback.classic

trait GenericConfig {
  val logLevel : Level = Level.ERROR

  def GetLogger(c: Class[_]) : Logger = {
    val logger = Logger(c)
    logger.underlying.asInstanceOf[classic.Logger].setLevel(logLevel)

    logger
  }
}


