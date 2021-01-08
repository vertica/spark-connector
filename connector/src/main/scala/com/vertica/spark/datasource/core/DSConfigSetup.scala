package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import org.apache.spark.sql.types.StructType

import ch.qos.logback.classic.Level

import com.vertica.spark.config._

import scala.util.Try
import scala.util.Success
import scala.util.Failure


/**
  * Interface for taking input of user selected options, performing any setup steps required, and returning the proper configuration structure for the operation.
  */
trait DSConfigSetupInterface[T] {
  /**
    * Validates and returns the configuration structure for the specific read/write operation.
    *
    * @return Will return an error if validation of the user options failed, otherwise will return the configuration structure expected by the writer/reader.
    */
  def validateAndGetConfig(): Either[Seq[ConnectorError], T]

  /**
    * Returns the schema for the table as required by Spark.
    */
  def getTableSchema: Either[ConnectorError, StructType]
}


/**
  * Util class for common config setup functionality.
  */
object DSConfigSetupUtils {

  /**
    * Parses the log level from options.
    */
  def getLogLevel(config: Map[String, String]): Either[ConnectorError, Level] = {
    config.get("logging_level").map {
      case "ERROR" => Right(Level.ERROR)
      case "DEBUG" => Right(Level.DEBUG)
      case "WARNING" => Right(Level.WARN)
      case "INFO" => Right(Level.INFO)
      case _ => Left(ConnectorError(InvalidLoggingLevel))
    }.getOrElse(Right(Level.ERROR))
  }

  def getHost(config: Map[String, String]): Either[ConnectorError, String] = {
    if(config.contains("host")) Right(config.getOrElse("host", "")) else Left(ConnectorError(HostMissingError))
  }

  def getPort(config: Map[String, String]): Either[ConnectorError, Integer] = {
    Try {config.getOrElse("port","5543").toInt} match {
      case Success(i) => if(i >= 1 && i <= 65535) Right(i) else Left(ConnectorError(InvalidPortError))
      case Failure(_) => Left(ConnectorError(InvalidPortError))
    }
  }

  def getDb(config: Map[String, String]): Either[ConnectorError, String] = {
    if(config.contains("db")) Right(config.getOrElse("db","")) else Left(ConnectorError(DbMissingError))
  }

  def getUser(config: Map[String, String]): Either[ConnectorError, String] = {
    if(config.contains("user")) Right(config.getOrElse("user","")) else Left(ConnectorError(UserMissingError))
    //TODO: make option once kerberos support is introduced
  }

  def getTablename(config: Map[String, String]): Either[ConnectorError, String] = {
    if(config.contains("tablename")) Right(config.getOrElse("tablename","")) else Left(ConnectorError(TablenameMissingError))
  }

  def getPassword(config: Map[String, String]): Either[ConnectorError, String] = {
    if(config.contains("password")) Right(config.getOrElse("password","")) else Left(ConnectorError(PasswordMissingError))
    //TODO: make option once kerberos support is introduced
  }

}

/**
  * Trait to be inhereted for parser classes. Allows for pattern of parsing several options and storing any errors encountered in a list.
  */
trait ConfigParser[ErrorType] {

  /**
   * List of configuration errors. We keep these all so that we report all issues with the given configuration to the user at once and they don't have to solve issues one by one.
   */
  var errorList : List[ErrorType] = List()

  /**
    * Takes an Either between a result and error type. Returns an option for the result type and adds any errors to the error list
    */
  def checkEitherError[DataType](either: Either[ErrorType, DataType]): Option[DataType] = {
    either match {
      case Right(value) =>
        Some(value)
      case Left(err) =>
        errorList = errorList :+ err
        None
    }
  }

  /**
    * Version of checkEitherError where a sequence of errors is an option rather than a single error. This sequence of errors is merged into the existing list of errors.
    */
  def checkEitherErrorList[DataType](either: Either[Seq[ErrorType], DataType]): Option[DataType] = {
    either match {
      case Right(value) =>
        Some(value)
      case Left(errList) =>
        errorList = errorList ++ errList
        None
    }
  }

  def resetErrList: Unit = {
    errorList = List()
  }
}

/**
  * Parser for JDBC configuration
  */
class JDBCConfigParser() extends ConfigParser[ConnectorError] {

  /**
    * Parses the config map for JDBC config params, collecting any errors.
    */
  def validateAndGetJDBCConfig(config: Map[String, String], logLevelOption: Option[Level]): Either[Seq[ConnectorError], JDBCConfig] = {
    resetErrList

    val hostOption = checkEitherError[String](DSConfigSetupUtils.getHost(config))

    val portOption = checkEitherError[Integer](DSConfigSetupUtils.getPort(config))

    val dbOption = checkEitherError[String](DSConfigSetupUtils.getDb(config))

    val userOption = checkEitherError[String](DSConfigSetupUtils.getUser(config))

    val passwordOption = checkEitherError[String](DSConfigSetupUtils.getPassword(config))

    if(errorList.nonEmpty) {
      Left(errorList)
    }
    else
    {
      // Sanity check: make sure required values exist
      (hostOption, portOption, dbOption, userOption, passwordOption, logLevelOption) match {
        case (Some(host), Some(port), Some(db), Some(user), Some(password), Some(loggingLevel)) =>
          Right(JDBCConfig(host=host, port=port, db=db, username=user, password=password, logLevel = loggingLevel))
        case (Some(host), Some(port), Some(db), Some(user), Some(password), None) =>
          Left(List())
        case _ =>
          Left(List(ConnectorError(ConfigBuilderError)))
      }
    }
  }
}

/**
  * Implementation for parsing user option map and getting read config
  *
  * Contains mutable state for building out the configuration from the user map, which it then turns into immutable state when validateAndGetConfig() is called.
  */
class DSReadConfigSetup(val config: Map[String, String]) extends DSConfigSetupInterface[ReadConfig] with ConfigParser[ConnectorError] {


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
  override def validateAndGetConfig(): Either[Seq[ConnectorError], ReadConfig] = {
    resetErrList

    loggingLevelOption = checkEitherError[Level](DSConfigSetupUtils.getLogLevel(config))

    jdbcConfigOption = checkEitherErrorList[JDBCConfig](new JDBCConfigParser().validateAndGetJDBCConfig(config, loggingLevelOption))

    tablenameOption = checkEitherError[String](DSConfigSetupUtils.getTablename(config))

    if(errorList.nonEmpty) {
      Left(errorList)
    }
    else
    {
      // Sanity check: make sure required values exist
      (loggingLevelOption, jdbcConfigOption, tablenameOption) match {
        case (Some(loggingLevel), Some(jdbcConfig), Some(tablename)) =>
          // First, create initial config without metadata
          val initialConfig = DistributedFilesystemReadConfig(logLevel = loggingLevel, jdbcConfig = jdbcConfig, tablename = tablename, metadata=None)

          // Get pipe
          val pipe = VerticaPipeFactory.getReadPipe(initialConfig)

          // Then, retrieve metadata
          pipe.getMetadata match {
            case Left(err) => Left(List(err))
            case Right(metadata) => Right(initialConfig.copy(metadata=Some(metadata)))
          }
        case _ => Left(List(ConnectorError(ConfigBuilderError)))
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
class DSWriteConfigSetup(val config: Map[String, String]) extends DSConfigSetupInterface[WriteConfig] with ConfigParser[ConnectorError]{

  // Configuration parameters (mandatory for config)
  var loggingLevelOption: Option[Level] = None

  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[ReadConfig]] or [[ConnectorError]]
    */
  override def validateAndGetConfig(): Either[Seq[ConnectorError], WriteConfig] = {
    // List of configuration errors. We keep these all so that we report all issues with the given configuration to the user at once and they don't have to solve issues one by one.
    resetErrList

    loggingLevelOption = checkEitherError[Level](DSConfigSetupUtils.getLogLevel(config))

    if(errorList.nonEmpty) {
      Left(errorList)
    }
    else
    {
      // Sanity check: make sure required values exist
      loggingLevelOption match {
        case Some(loggingLevel) => Right(DistributedFilesystemWriteConfig(logLevel = loggingLevel))
        case _ => Left(List(ConnectorError(ConfigBuilderError)))
      }
    }
  }

  override def getTableSchema: Either[ConnectorError, StructType] = ???
}

