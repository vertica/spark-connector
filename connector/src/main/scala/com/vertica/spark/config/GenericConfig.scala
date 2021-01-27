package com.vertica.spark.config

import com.typesafe.scalalogging.Logger
import ch.qos.logback.classic.Level
import ch.qos.logback.classic

import scala.util.Try
import scala.util.Success
import scala.util.Failure

final case class LogProvider(logLevel: Level) {
  def getLogger(c: Class[_]): Logger = {
    val logger = Logger(c)
    Try{logger.underlying.asInstanceOf[classic.Logger].setLevel(logLevel) } match {
      case Failure(_) => logger.error("Could not set log level based on configuration.")
      case Success(_) => ()
    }

    logger
  }
}

/**
  * Generic config that all operations (read and write) share
  */
trait GenericConfig {
  val logLevel: Level = Level.ERROR
  val logProvider: LogProvider = LogProvider(logLevel)

  def getLogger(c: Class[_]): Logger = logProvider.getLogger(c)
}


