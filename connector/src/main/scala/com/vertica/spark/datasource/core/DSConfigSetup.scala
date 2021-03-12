// (c) Copyright [2020-2021] Micro Focus or one of its affiliates.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.vertica.spark.datasource.core

import com.vertica.spark.util.error._
import org.apache.spark.sql.types.StructType
import ch.qos.logback.classic.Level
import com.vertica.spark.config._

import scala.util.Try
import scala.util.Success
import scala.util.Failure
import cats.data._
import cats.data.Validated._
import cats.implicits._
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult


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
   *
   * @return Optionally returns partitioning information for the operation when needed
   */
  def performInitialSetup(config: T): ConnectorResult[Option[PartitionInfo]]

  /**
    * Returns the schema for the table as required by Spark.
    */
  def getTableSchema(config: T): ConnectorResult[StructType]
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
      case _ => InvalidLoggingLevel().invalidNec
    }.getOrElse(Level.ERROR.validNec)
  }

  def getHost(config: Map[String, String]): ValidationResult[String] = {
    config.get("host") match {
      case Some(host) => host.validNec
      case None => HostMissingError().invalidNec
    }
  }

  def getStagingFsUrl(config: Map[String, String]): ValidationResult[String] = {
    config.get("staging_fs_url") match {
      case Some(address) => address.validNec
      case None => StagingFsUrlMissingError().invalidNec
    }
  }

  def getPort(config: Map[String, String]): ValidationResult[Int] = {
    Try {config.getOrElse("port","5433").toInt} match {
      case Success(i) => if (i >= 1 && i <= 65535) i.validNec else InvalidPortError().invalidNec
      case Failure(_) => InvalidPortError().invalidNec
    }
  }

  def getFailedRowsPercentTolerance(config: Map[String, String]): ValidationResult[Float] = {
    Try {config.getOrElse("failed_rows_percent_tolerance","0.00").toFloat} match {
      case Success(f) => if (f >= 0.00 && f <= 1.00) f.validNec else InvalidFailedRowsTolerance().invalidNec
      case Failure(_) => InvalidFailedRowsTolerance().invalidNec
    }
  }

  def getDb(config: Map[String, String]): ValidationResult[String] = {
    config.get("db") match {
      case Some(db) => db.validNec
      case None => DbMissingError().invalidNec
    }
  }

  def getUser(config: Map[String, String]): ValidationResult[String] = {
    config.get("user") match {
      case Some(user) => user.validNec
      case None => UserMissingError().invalidNec
    }
    //TODO: make option once kerberos support is introduced
  }

  def getTablename(config: Map[String, String]): ValidationResult[String] = {
    config.get("table") match {
      case Some(table) => table.validNec
      case None => TablenameMissingError().invalidNec
    }
  }

  def getDbSchema(config: Map[String, String]): ValidationResult[Option[String]] = {
    config.get("dbschema") match {
      case Some(dbschema) => Some(dbschema).validNec
      case None => None.validNec
    }
  }

  def getPassword(config: Map[String, String]): ValidationResult[String] = {
    config.get("password") match {
      case Some(password) => password.validNec
      case None => PasswordMissingError().invalidNec
    }
    //TODO: make option once kerberos support is introduced
  }

  def getTargetTableSQL(config: Map[String, String]): ValidationResult[Option[String]] = {
    config.get("target_table_sql").validNec
  }



  def getCopyColumnList(config: Map[String, String]): ValidationResult[Option[ValidColumnList]] = {
    config.get("copy_column_list") match {
      case None => None.validNec
      case Some(listStr) => ValidColumnList(listStr)
    }
  }

  // Optional param, if not specified the partition count will be decided as part of the inital steps
  def getPartitionCount(config: Map[String, String]): ValidationResult[Option[Int]] = {
    config.get("num_partitions") match {
      case Some(partitionCount) => Try{partitionCount.toInt} match {
        case Success(i) =>
          if(i > 0) Some(i).validNec else InvalidPartitionCountError().invalidNec
        case Failure(_) => InvalidPartitionCountError().invalidNec
      }
      case None => None.validNec
    }
  }

  def getStrLen(config: Map[String, String]) : ValidationResult[Long] = {
    Try {config.getOrElse("strlen","1024").toLong} match {
      case Success(i) => if (i >= 1 && i <= 32000000) i.validNec else InvalidStrlenError().invalidNec
      case Failure(_) => InvalidStrlenError().invalidNec
    }
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

  def validateAndGetFilestoreConfig(config: Map[String, String], logLevel: Level, sessionId: String): DSConfigSetupUtils.ValidationResult[FileStoreConfig] = {
    DSConfigSetupUtils.getStagingFsUrl(config).map(
      address => {
        val delimiter = if(address.takeRight(1) == "/" || address.takeRight(1) == "\\") "" else "/"
        val uniqueSessionId = sessionId

        // Create unique directory for session
        val uniqueAddress = address.stripSuffix(delimiter) + delimiter + uniqueSessionId

        FileStoreConfig(uniqueAddress, logLevel)
      }
    )
  }

  def validateAndGetFullTableName(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[TableName] = {
    (DSConfigSetupUtils.getTablename(config),
      DSConfigSetupUtils.getDbSchema(config) ).mapN(TableName)
  }

}

/**
  * Implementation for parsing user option map and getting read config
  */
class DSReadConfigSetup(val pipeFactory: VerticaPipeFactoryInterface = VerticaPipeFactory, val sessionIdInterface: SessionIdInterface = SessionId) extends DSConfigSetupInterface[ReadConfig] {
  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[ReadConfig]] or sequence of [[ConnectorError]]
    */
  override def validateAndGetConfig(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[ReadConfig] = {
    val sessionId = sessionIdInterface.getId

    DSConfigSetupUtils.validateAndGetJDBCConfig(config).andThen { jdbcConfig =>
      DSConfigSetupUtils.validateAndGetFilestoreConfig(config, jdbcConfig.logLevel, sessionId).andThen { fileStoreConfig =>
        DSConfigSetupUtils.validateAndGetFullTableName(config).andThen { tableName =>
            (jdbcConfig.logLevel.validNec,
            jdbcConfig.validNec,
            fileStoreConfig.validNec,
            tableName.validNec,
            DSConfigSetupUtils.getPartitionCount(config),
            None.validNec).mapN(DistributedFilesystemReadConfig).andThen { initialConfig =>
              val pipe = pipeFactory.getReadPipe(initialConfig)

              // Then, retrieve metadata
              val metadata = pipe.getMetadata
               metadata match {
                case Left(err) => err.invalidNec
                case Right(meta) => meta match {
                  case readMeta: VerticaReadMetadata => initialConfig.copy(metadata = Some(readMeta)).validNec
                  case _ => MissingMetadata().invalidNec
                }
              }
          }
        }
      }
    }
  }

  override def performInitialSetup(config: ReadConfig): ConnectorResult[Option[PartitionInfo]] = {
    pipeFactory.getReadPipe(config).doPreReadSteps() match {
      case Right(partitionInfo) => Right(Some(partitionInfo))
      case Left(err) => Left(err)
    }
  }

  override def getTableSchema(config: ReadConfig): ConnectorResult[StructType] =  {
    config match {
      case DistributedFilesystemReadConfig(_, _, _, _, _, verticaMetadata) =>
        verticaMetadata match {
          case None => Left(SchemaDiscoveryError(None))
          case Some(metadata) => Right(metadata.schema)
        }
    }
  }
}

/**
  * Implementation for parsing user option map and getting write config
  */
class DSWriteConfigSetup(val schema: Option[StructType], val pipeFactory: VerticaPipeFactoryInterface = VerticaPipeFactory, sessionIdInterface: SessionIdInterface = SessionId) extends DSConfigSetupInterface[WriteConfig] {
  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[WriteConfig]] or [[ConnectorError]]
    */
  override def validateAndGetConfig(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[WriteConfig] = {
    val sessionId = sessionIdInterface.getId

    // List of configuration errors. We keep these all so that we report all issues with the given configuration to the user at once and they don't have to solve issues one by one.
    DSConfigSetupUtils.validateAndGetJDBCConfig(config).andThen { jdbcConfig =>
      DSConfigSetupUtils.validateAndGetFilestoreConfig(config, jdbcConfig.logLevel, sessionId).andThen { fileStoreConfig =>
        DSConfigSetupUtils.validateAndGetFullTableName(config).andThen { tableName =>
          schema match {
            case Some(passedInSchema) =>
              (jdbcConfig.logLevel.validNec,
                jdbcConfig.validNec,
                fileStoreConfig.validNec,
                tableName.validNec,
                passedInSchema.validNec,
                DSConfigSetupUtils.getStrLen(config),
                DSConfigSetupUtils.getTargetTableSQL(config),
                DSConfigSetupUtils.getCopyColumnList(config),
                sessionId.validNec,
                DSConfigSetupUtils.getFailedRowsPercentTolerance(config)
                ).mapN(DistributedFilesystemWriteConfig)
            case None =>
              MissingSchemaError().invalidNec
          }
        }
      }
    }
  }

  override def performInitialSetup(config: WriteConfig): ConnectorResult[Option[PartitionInfo]] = {
    val pipe = pipeFactory.getWritePipe(config)
    pipe.doPreWriteSteps() match {
      case Left(err) => Left(err)
      case Right(_) => Right(None)
    }
  }

  override def getTableSchema(config: WriteConfig): ConnectorResult[StructType] = this.schema match {
    case Some(schema) => Right(schema)
    case None => Left(SchemaDiscoveryError(None))
  }
}
