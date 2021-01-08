package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import org.apache.spark.sql.types.StructType

import ch.qos.logback.classic.Level

import com.vertica.spark.config._

import scala.util.Try
import scala.util.Success
import scala.util.Failure
import cats.data._
import cats.data.Validated._
import cats.implicits._


/**
  * Interface for taking input of user selected options, performing any setup steps required, and returning the proper configuration structure for the operation.
  */
trait DSConfigSetupInterface[T] {
  /**
    * Validates and returns the configuration structure for the specific read/write operation.
    *
    * @return Will return an error if validation of the user options failed, otherwise will return the configuration structure expected by the writer/reader.
    */
  def validateAndGetConfig(): DSConfigSetupUtils.ValidationResult[T]

  /**
    * Returns the schema for the table as required by Spark.
    */
  def getTableSchema: Either[ConnectorError, StructType]
}


/**
  * Util class for common config setup functionality.
  */
object DSConfigSetupUtils {
  type ValidationResult[A] = ValidatedNec[ConnectorError, A]
  /**
    * Parses the log level from options.
    */
  def getLogLevel(config: Map[String, String]): ValidationResult[Level] = {
    config.get("logging_level").map {
      case "ERROR" => Level.ERROR.validNec
      case "DEBUG" => Level.DEBUG.validNec
      case "WARNING" => Level.WARN.validNec
      case "INFO" => Level.INFO.validNec
      case _ => ConnectorError(InvalidLoggingLevel).invalidNec
    }.getOrElse(Level.ERROR.validNec)
  }

  def getHost(config: Map[String, String]): ValidationResult[String] = {
    config.get("host") match {
      case Some(host) => host.validNec
      case None => ConnectorError(HostMissingError).invalidNec
    }
  }

  def getPort(config: Map[String, String]): ValidationResult[Int] = {
    Try {config.getOrElse("port","5543").toInt} match {
      case Success(i) => if (i >= 1 && i <= 65535) i.validNec else ConnectorError(InvalidPortError).invalidNec
      case Failure(_) => ConnectorError(InvalidPortError).invalidNec
    }
  }

  def getDb(config: Map[String, String]): ValidationResult[String] = {
    config.get("db") match {
      case Some(db) => db.validNec
      case None => ConnectorError(DbMissingError).invalidNec
    }
  }

  def getUser(config: Map[String, String]): ValidationResult[String] = {
    config.get("user") match {
      case Some(user) => user.validNec
      case None => ConnectorError(UserMissingError).invalidNec
    }
    //TODO: make option once kerberos support is introduced
  }

  def getTablename(config: Map[String, String]): ValidationResult[String] = {
    config.get("tablename") match {
      case Some(tablename) => tablename.validNec
      case None => ConnectorError(TablenameMissingError).invalidNec
    }
  }

  def getPassword(config: Map[String, String]): ValidationResult[String] = {
    config.get("password") match {
      case Some(password) => password.validNec
      case None => ConnectorError(PasswordMissingError).invalidNec
    }
    //TODO: make option once kerberos support is introduced
  }

  /**
   * Parses the config map for JDBC config params, collecting any errors.
   */
  def validateAndGetJDBCConfig(config: Map[String, String], logLevel: Level): DSConfigSetupUtils.ValidationResult[JDBCConfig] = {
    (DSConfigSetupUtils.getHost(config),
    DSConfigSetupUtils.getPort(config),
    DSConfigSetupUtils.getDb(config),
    DSConfigSetupUtils.getUser(config),
    DSConfigSetupUtils.getPassword(config),
    logLevel.validNec).mapN(JDBCConfig)
  }
}

/**
  * Implementation for parsing user option map and getting read config
  *
  * Contains mutable state for building out the configuration from the user map, which it then turns into immutable state when validateAndGetConfig() is called.
  */
class DSReadConfigSetup(val config: Map[String, String]) extends DSConfigSetupInterface[ReadConfig] {


  // Configuration parameters (mandatory for config)
  var loggingLevelOption: Option[Level] = None
  var jdbcConfigOption: Option[JDBCConfig] = None
  var tablenameOption: Option[String] = None
  var verticaMetadata: Option[VerticaMetadata] = None

  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[ReadConfig]] or sequence of [[ConnectorError]]
    */
  override def validateAndGetConfig(): DSConfigSetupUtils.ValidationResult[ReadConfig] = {
    DSConfigSetupUtils.getLogLevel(config).andThen{ logLevel: Level =>
      (DSConfigSetupUtils.getLogLevel(config),
      DSConfigSetupUtils.validateAndGetJDBCConfig(config, logLevel),
      DSConfigSetupUtils.getTablename(config),
      None.validNec).mapN(DistributedFilesystemReadConfig).andThen{ initialConfig =>
        val pipe = VerticaPipeFactory.getReadPipe(initialConfig)

        // Then, retrieve metadata
        pipe.getMetadata.toValidatedNec.map(metadata => initialConfig.copy(metadata = Some(metadata)))
      }
    }
  }

  override def getTableSchema: Either[ConnectorError, StructType] =  {
    verticaMetadata match {
      case None => Left(ConnectorError(SchemaDiscoveryError))
      case Some(metadata) => Right(metadata.schema)
    }
  }
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
  override def validateAndGetConfig(): DSConfigSetupUtils.ValidationResult[WriteConfig] = {
    // List of configuration errors. We keep these all so that we report all issues with the given configuration to the user at once and they don't have to solve issues one by one.
    DSConfigSetupUtils.getLogLevel(config).map(DistributedFilesystemWriteConfig)
  }

  override def getTableSchema: Either[ConnectorError, StructType] = ???
}