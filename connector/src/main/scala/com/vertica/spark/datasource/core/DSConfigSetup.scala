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
  def validateAndGetConfig(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[T]

  /**
    * Performs any necessary initial steps required for the given configuration
    */
  def performInitialSetup(config: T): Either[ConnectorError, Unit]

  /**
    * Returns the schema for the table as required by Spark.
    */
  def getTableSchema(config: T): Either[ConnectorError, StructType]
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

  def getStagingFsUrl(config: Map[String, String]): ValidationResult[String] = {
    config.get("staging_fs_url") match {
      case Some(address) => address.validNec
      case None => ConnectorError(StagingFsUrlMissingError).invalidNec
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
  def validateAndGetJDBCConfig(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[JDBCConfig] = {
    (DSConfigSetupUtils.getHost(config),
    DSConfigSetupUtils.getPort(config),
    DSConfigSetupUtils.getDb(config),
    DSConfigSetupUtils.getUser(config),
    DSConfigSetupUtils.getPassword(config),
    DSConfigSetupUtils.getLogLevel(config)).mapN(JDBCConfig)
  }

  def validateAndGetFilestoreConfig(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[FileStoreConfig] = {
    DSConfigSetupUtils.getStagingFsUrl(config).map(address => FileStoreConfig(address))
  }

}

/**
  * Implementation for parsing user option map and getting read config
  */
object DSReadConfigSetup extends DSConfigSetupInterface[ReadConfig] {
  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[ReadConfig]] or sequence of [[ConnectorError]]
    */
  override def validateAndGetConfig(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[ReadConfig] = {
    DSConfigSetupUtils.validateAndGetJDBCConfig(config).andThen { jdbcConfig =>
      DSConfigSetupUtils.validateAndGetFilestoreConfig(config).andThen { fileStoreConfig =>
        (jdbcConfig.logLevel.validNec,
        jdbcConfig.validNec,
        fileStoreConfig.validNec,
        DSConfigSetupUtils.getTablename(config),
        None.validNec).mapN(DistributedFilesystemReadConfig).andThen { initialConfig =>
          val pipe = VerticaPipeFactory.getReadPipe(initialConfig)

          // Then, retrieve metadata
          pipe.getMetadata.toValidatedNec.map(metadata => initialConfig.copy(metadata = Some(metadata)))
        }
      }
    }
  }

  override def performInitialSetup(config: ReadConfig): Either[ConnectorError, Unit] = {
    VerticaPipeFactory.getReadPipe(config).doPreReadSteps() match {
      case Right(_) => Right(())
      case Left(err) => Left(err)
    }
  }

  override def getTableSchema(config: ReadConfig): Either[ConnectorError, StructType] =  {
    config match {
      case DistributedFilesystemReadConfig(_, _, _, _, verticaMetadata) =>
        verticaMetadata match {
          case None => Left(ConnectorError(SchemaDiscoveryError))
          case Some(metadata) => Right(metadata.schema)
        }
    }
  }
}

/**
  * Implementation for parsing user option map and getting write config
  */
object DSWriteConfigSetup extends DSConfigSetupInterface[WriteConfig] {
  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[WriteConfig]] or [[ConnectorError]]
    */
  override def validateAndGetConfig(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[WriteConfig] = {
    // List of configuration errors. We keep these all so that we report all issues with the given configuration to the user at once and they don't have to solve issues one by one.
    DSConfigSetupUtils.getLogLevel(config).map(DistributedFilesystemWriteConfig)
  }

  override def performInitialSetup(config: WriteConfig): Either[ConnectorError, Unit] = Right(())

  override def getTableSchema(config: WriteConfig): Either[ConnectorError, StructType] = ???
}
