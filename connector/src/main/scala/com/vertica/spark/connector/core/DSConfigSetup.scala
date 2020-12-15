package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import org.apache.spark.sql.types.StructType

import com.typesafe.scalalogging.Logger
import ch.qos.logback.classic.Level

import com.vertica.spark.config._



trait DSConfigSetupInterface[T] {
  def validateAndGetConfig() : Either[ConnectorError, T]

  def getTableSchema() : Either[ConnectorError, StructType]
}

object DSConfigSetupUtils {

  def getLogLevel(config: Map[String, String]) : Option[Level] = {
    config.get("logging_level").map {
      case "ERROR" => Some(Level.ERROR)
      case "DEBUG" => Some(Level.DEBUG)
      case "WARNING" => Some(Level.WARN)
      case "INFO" => Some(Level.INFO)
      case _ => None
    }.getOrElse(Some(Level.ERROR))
  }
}

class DSReadConfigSetup(val config: Map[String, String]) extends DSConfigSetupInterface[ReadConfig] {
  override def validateAndGetConfig(): Either[ConnectorError, ReadConfig] = {
    val logLevel = DSConfigSetupUtils.getLogLevel(config)
    logLevel match {
      case Some(level) => Right(DistributedFilestoreReadConfig(level))
      case None => return Left(ConnectorError(LOGGING_LEVEL_PARSE_ERR))
    }
  }

  override def getTableSchema(): Either[ConnectorError, StructType] = ???
}

class DSWriteConfigSetup(val config: Map[String, String]) extends DSConfigSetupInterface[WriteConfig] {
  override def validateAndGetConfig(): Either[ConnectorError, WriteConfig] = {
    val logLevel = DSConfigSetupUtils.getLogLevel(config)
    logLevel match {
      case Some(level) => Right(DistributedFilestoreWriteConfig(level))
      case None => return Left(ConnectorError(LOGGING_LEVEL_PARSE_ERR))
    }
  }

  override def getTableSchema(): Either[ConnectorError, StructType] = ???
}

