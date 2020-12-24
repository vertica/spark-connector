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
  def validateAndGetConfig(): Either[Seq[ConnectorError], T]

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

  def getHost(config: Map[String, String]): Either[ConnectorError, String] = {
    if(config.contains("host")) Right(config.get("host").getOrElse("")) else Left(ConnectorError(HostMissingError))
  }

  def getPort(config: Map[String, String]): Either[ConnectorError, Integer] = {
    Right(config.get("port").getOrElse("5543").toInt) // default value
    // TODO: check if number
  }

  def getDb(config: Map[String, String]): Either[ConnectorError, String] = {
    if(config.contains("db")) Right(config.get("db").getOrElse("")) else Left(ConnectorError(DbMissingError))
  }

  def getUser(config: Map[String, String]): Either[ConnectorError, String] = {
    if(config.contains("user")) Right(config.get("user").getOrElse("")) else Left(ConnectorError(UserMissingError))
    //TODO: make option once kerberos support is introduced
  }

  def getPassword(config: Map[String, String]): Either[ConnectorError, String] = {
    if(config.contains("password")) Right(config.get("password").getOrElse("")) else Left(ConnectorError(PasswordMissingError))
    //TODO: make option once kerberos support is introduced
  }

  def validateAndGetJDBCConfig(config: Map[String, String], logLevelOption: Option[Level]): Either[Seq[ConnectorError], JDBCConfig] = {
    var errorList : List[ConnectorError] = List()

    // A little much duplication here. Tried to factor it out with a function to make it cleaner, but the issue with that is that you can't assign a tuple to mutable state in one line
    // val (x,y) = func() works,
    // (x,y) = func() does not
    // Where x,y are the parsed value and the error list
    // Open to suggestion here
    var hostOption: Option[String] = getHost(config) match {
      case Right(host) => Some(host)
      case Left(err) => {
        errorList = errorList :+ err
        None
      }
    }
    var portOption: Option[Integer] = getPort(config) match {
      case Right(port) => Some(port)
      case Left(err) => {
        errorList = errorList :+ err
        None
      }
    }
    var dbOption: Option[String] = getDb(config) match {
      case Right(db) => Some(db)
      case Left(err) => {
        errorList = errorList :+ err
        None
      }
    }
    var userOption: Option[String] = getUser(config) match {
      case Right(user) => Some(user)
      case Left(err) => {
        errorList = errorList :+ err
        None
      }
    }
    var passwordOption: Option[String] = getPassword(config) match {
      case Right(user) => Some(user)
      case Left(err) => {
        errorList = errorList :+ err
        None
      }
    }

    if(errorList.size > 0) {
      Left(errorList)
    }
    else
    {
      // Sanity check: make sure required values exist
      (hostOption, portOption, dbOption, userOption, passwordOption, logLevelOption) match {
        case (Some(host), Some(port), Some(db), Some(user), Some(password), Some(loggingLevel)) => {
          Right(JDBCConfig(host=host, port=port, db=db, username=user, password=password, logLevel = loggingLevel))
        }
        case _ => {
          Left(List(ConnectorError(ConfigBuilderError)))
        }
      }
    }
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

  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[ReadConfig]] or sequence of [[ConnectorError]]
    */
  override def validateAndGetConfig(): Either[Seq[ConnectorError], ReadConfig] = {
    // List of configuration errors. We keep these all so that we report all issues with the given configuration to the user at once and they don't have to solve issues one by one.
    var errorList : List[ConnectorError] = List()

    loggingLevelOption = DSConfigSetupUtils.getLogLevel(config) match {
      case Some(level) => Some(level)
      case None => {
        errorList = errorList :+ ConnectorError(InvalidLoggingLevel)
        None
      }
    }

    jdbcConfigOption = DSConfigSetupUtils.validateAndGetJDBCConfig(config, loggingLevelOption) match {
      case Right(jdbcConfig) => Some(jdbcConfig)
      case Left(errList) => {
        errorList = errorList ++ errList
        None
      }
    }


    if(errorList.size > 0) {
      Left(errorList)
    }
    else
    {
      // Sanity check: make sure required values exist
      (loggingLevelOption) match {
        case (Some(loggingLevel)) => Right(DistributedFilestoreReadConfig(logLevel = loggingLevel))
        case _ => Left(List(ConnectorError(ConfigBuilderError)))
      }
    }
  }

  override def getTableSchema(): Either[ConnectorError, StructType] = ???
    // readConfig.metada.schema
}

/**
  * Implementation for parsing user option map and getting write config
  */
class DSWriteConfigSetup(val config: Map[String, String]) extends DSConfigSetupInterface[WriteConfig] {

  // Configuration parameters (mandatory for config)
  var loggingLevelOption: Option[Level] = None

  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[ReadConfig]] or [[ConnectorError]]
    */
  override def validateAndGetConfig(): Either[Seq[ConnectorError], WriteConfig] = {
    // List of configuration errors. We keep these all so that we report all issues with the given configuration to the user at once and they don't have to solve issues one by one.
    var errorList : List[ConnectorError] = List()

    loggingLevelOption = DSConfigSetupUtils.getLogLevel(config) match {
      case Some(level) => Some(level)
      case None => {
        errorList = errorList :+ ConnectorError(InvalidLoggingLevel)
        None
      }
    }

    if(errorList.size > 0) {
      Left(errorList)
    }
    else
    {
      // Sanity check: make sure required values exist
      (loggingLevelOption) match {
        case (Some(loggingLevel)) => Right(DistributedFilestoreWriteConfig(logLevel = loggingLevel))
        case _ => Left(List(ConnectorError(ConfigBuilderError)))
      }
    }
  }

  override def getTableSchema(): Either[ConnectorError, StructType] = ???
}

