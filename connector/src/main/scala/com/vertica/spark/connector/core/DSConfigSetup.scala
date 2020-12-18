package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import org.apache.spark.sql.types.StructType

import com.typesafe.scalalogging.Logger
import ch.qos.logback.classic.Level

import com.vertica.spark.config._



/**
  * Interface for taking input of user selected options, performing any setup steps required, and returning the proper configuration structure for the operation.
  */
trait DSConfigSetupInterface[T] {
  /**
    * Validates and returns the configuration structure for the specific read/write operation.
    *
    * @return Will return an error if validation of the user options failed, otherwise will return the configuration structure expected by the writer/reader.
    */
  def validateAndGetConfig(): Either[ConnectorError, T]

  /**
    * Returns the schema for the table as required by Spark.
    */
  def getTableSchema(): Either[ConnectorError, StructType]
}


/**
  * Util class for common config setup functionality.
  */
object DSConfigSetupUtils {

  /**
    * Parses the log level from options.
    */
  def getLogLevel(config: Map[String, String]): Option[Level] = {
    config.get("logging_level").map {
      case "ERROR" => Some(Level.ERROR)
      case "DEBUG" => Some(Level.DEBUG)
      case "WARNING" => Some(Level.WARN)
      case "INFO" => Some(Level.INFO)
      case _ => None
    }.getOrElse(Some(Level.ERROR))
  }
}

/**
  * Implementation for parsing user option map and getting read config
  */
class DSReadConfigSetup(val config: Map[String, String]) extends DSConfigSetupInterface[ReadConfig] {

  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[ReadConfig]] or [[ConnectorError]]
    */
  override def validateAndGetConfig(): Either[ConnectorError, ReadConfig] = {
    val logLevel = DSConfigSetupUtils.getLogLevel(config)
    logLevel match {
      case Some(level) => Right(DistributedFilestoreReadConfig(level))
      case None => Left(ConnectorError(LOGGING_LEVEL_PARSE_ERR))
    }
  }

  override def getTableSchema(): Either[ConnectorError, StructType] = ???
}

/**
  * Implementation for parsing user option map and getting write config
  */
class DSWriteConfigSetup(val config: Map[String, String]) extends DSConfigSetupInterface[WriteConfig] {

  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[ReadConfig]] or [[ConnectorError]]
    */
  override def validateAndGetConfig(): Either[ConnectorError, WriteConfig] = {
    val logLevel = DSConfigSetupUtils.getLogLevel(config)
    logLevel match {
      case Some(level) => Right(DistributedFilestoreWriteConfig(level))
      case None => Left(ConnectorError(LOGGING_LEVEL_PARSE_ERR))
    }
  }

  override def getTableSchema(): Either[ConnectorError, StructType] = ???
}

