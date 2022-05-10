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
import com.vertica.spark.config._

import scala.util.Try
import scala.util.Success
import scala.util.Failure
import cats.data._
import cats.data.Validated._
import cats.implicits._
import com.typesafe.scalalogging.Logger
import com.vertica.spark.datasource.core.factory.{VerticaPipeFactory, VerticaPipeFactoryInterface}
import com.vertica.spark.datasource.fs.{GCSConnectorOptions, GCSSparkOptions, GCSEnvVars}
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.spark.sql.SparkSession


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

sealed trait TLSMode
case object Disable extends TLSMode {
  override def toString: String = "disable"
}
case object Require extends TLSMode {
  override def toString: String = "require"
}
case object VerifyCA extends TLSMode {
  override def toString: String = "verify-ca"
}
case object VerifyFull extends TLSMode {
  override def toString: String = "verify-full"
}

sealed trait CreateExternalTableOption
case object ExistingData extends CreateExternalTableOption {
  override def toString: String = "existing-data"
}
case object NewData extends CreateExternalTableOption {
  override def toString: String = "new-data"
}

/**
  * Util class for common config setup functionality.
  */
// scalastyle:off
object DSConfigSetupUtils {
  type ValidationResult[+A] = ValidatedNec[ConnectorError, A]

  def checkOldConnectorOptions(config: Map[String, String]): Seq[ConnectorError] = {
    val oldList = Array("target_table_ddl", "numpartitions", "hdfs_url", "web_hdfs_url")
    val replacementsList = Array("target_table_sql", "num_partitions", "staging_fs_url", "staging_fs_url")

    oldList.zip(replacementsList).filter({
      case (old, replacement) => config.contains(old)
    }).map {
      case (old, replacement) =>
          V1ReplacementOption(old, replacement)
    }
  }

  def logOrAppendErrorsForOldConnectorOptions[T](config: Map[String, String], res: ValidationResult[T], logger: Logger): ValidationResult[T] = {
    val oldConnectorMessages = DSConfigSetupUtils.checkOldConnectorOptions(config)
    val oldConnectorChain = NonEmptyChain.fromChain(Chain.fromSeq(oldConnectorMessages))

    // These are not fatal errors, only add to errors if there is something missing from configuration.
    // Otherwise, just log a warning
    res match {
      case Valid(a) =>
        oldConnectorMessages.foreach(m => logger.warn(m.getUserMessage))
        Valid(a)
      case Invalid(errList) => Invalid(
        oldConnectorChain match {
          case Some(chain) => errList.concat(chain)
          case None => errList
        }
      )
    }
  }

  def getHost(config: Map[String, String]): ValidationResult[String] = {
    config.get("host") match {
      case Some(host) => host.validNec
      case None => HostMissingError().invalidNec
    }
  }

  def getCreateExternalTable(config: Map[String, String]): ValidationResult[Option[CreateExternalTableOption]] = {
    config.get("create_external_table") match {
      case Some(str) =>
        str match {
          case "new-data" | "true" => Some(NewData).validNec
          case "existing-data" => Some(ExistingData).validNec
          case _ => InvalidCreateExternalTableOption().invalidNec
        }
      case None => None.validNec
    }
  }

  def getSaveJobStatusTable(config: Map[String, String]): ValidationResult[Boolean] = {
    config.get("save_job_status_table") match {
      case Some(str) =>
        str match {
          case "true" => true.validNec
          case "false" => false.validNec
          case _ => InvalidSaveJobStatusTableOption().invalidNec
        }
      case None => false.validNec
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

  def getMaxFileSize(config: Map[String, String]): ValidationResult[Int] = {
    Try {config.getOrElse("max_file_size_export_mb","4096").toInt} match {
      case Success(i) => i.validNec
      case Failure(_) => InvalidIntegerField("max_file_size_export_mb").invalidNec
    }
  }

  def getMaxRowGroupSize(config: Map[String, String]): ValidationResult[Int] = {
    Try {config.getOrElse("max_row_group_size_export_mb","16").toInt} match {
      case Success(i) => i.validNec
      case Failure(_) => InvalidIntegerField("max_row_group_size_export_mb").invalidNec
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

  def getUser(config: Map[String, String]): Option[String] = {
    config.get("user")
  }

  def getPassword(config: Map[String, String]): Option[String] = {
    config.get("password")
  }

  def getKerberosServiceName(config: Map[String, String]): Option[String] = {
    config.get("kerberos_service_name")
  }

  def getKerberosHostname(config: Map[String, String]): Option[String] = {
    config.get("kerberos_host_name")
  }

  def getJaasConfigName(config: Map[String, String]): Option[String] = {
    config.get("jaas_config_name")
  }

  def getTLS(config: Map[String, String]): ValidationResult[TLSMode] = {
    config.get("tls_mode") match {
      case Some(value) => value match {
        case "disable" => Disable.validNec
        case "require" => Require.validNec
        case "verify-ca" => VerifyCA.validNec
        case "verify-full" => VerifyFull.validNec
        case _ => TLSModeParseError().invalidNec
      }
      case None => Disable.validNec
    }
  }

  def getAWSAuth(config: Map[String, String]): ValidationResult[Option[AWSAuth]] = {
    val visibility = Secret
    val accessKeyIdOpt = getSensitiveOption(visibility)(
      config,
      "aws_access_key_id",
      "spark.hadoop.fs.s3a.access.key",
      "AWS_ACCESS_KEY_ID").sequence
    val secretAccessKeyOpt = getSensitiveOption(visibility)(
      config,
      "aws_secret_access_key",
      "spark.hadoop.fs.s3a.secret.key",
      "AWS_SECRET_ACCESS_KEY").sequence
    (accessKeyIdOpt, secretAccessKeyOpt) match {
      case (Some(accessKeyId), Some(secretAccessKey)) => (accessKeyId, secretAccessKey).mapN(AWSAuth).map(Some(_))
      case (None, None) => None.validNec
      case (Some(_), None) => MissingAWSAccessKeyId().invalidNec
      case (None, Some(_)) => MissingAWSSecretAccessKey().invalidNec
    }
  }

  def getGCSKeyFile(config: Map[String, String]): ValidationResult[Option[SensitiveArg[String]]] = {
    val visibility = Secret
    getSensitiveOption(visibility)(
      config,
      GCSConnectorOptions.SERVICE_JSON_KEYFILE,
      GCSSparkOptions.SERVICE_JSON_KEYFILE,
      GCSEnvVars.SERVICE_JSON_KEYFILE
    )
  }

  def getGCSServiceAccountAuth(config: Map[String, String]): ValidationResult[Option[GCSServiceAuth]] = {
    val visibility = Secret

    val keyIdFound = getSensitiveOptionWithName(visibility)(
      config,
      GCSConnectorOptions.SERVICE_KEY_ID,
      GCSSparkOptions.SERVICE_ACC_KEY_ID,
      GCSEnvVars.SERVICE_KEY_ID
    )

    val keySecretFound = getSensitiveOptionWithName(visibility)(
      config,
      GCSConnectorOptions.SERVICE_KEY,
      GCSSparkOptions.SERVICE_ACC_KEY,
      GCSEnvVars.SERVICE_KEY
    )

    val emailFound = getSensitiveOptionWithName(visibility)(
      config,
      GCSConnectorOptions.SERVICE_EMAIL,
      GCSSparkOptions.SERVICE_ACC_EMAIL,
      GCSEnvVars.SERVICE_EMAIL
    )

    val results = List(keyIdFound, keySecretFound, emailFound)
    val invalids = results.filter(_.option.isInvalid)
    if(invalids.isEmpty){
      (keyIdFound.option.toOption.get, keySecretFound.option.toOption.get, emailFound.option.toOption.get)
        .mapN(GCSServiceAuth).validNec
    } else {
      val found = results.filter(_.option.isValid).map(_.optionName)
      val missing = invalids.map(_.optionName)
      MissingGCSServiceAccountAuthentications(found, missing).invalidNec
    }
  }

  def getVerticaGCSAuth(config: Map[String, String]): ValidationResult[Option[GCSVerticaAuth]] = {
    val visibility = Secret
    val accessKeyIdOpt = getSensitiveOption(visibility)(
      config,
      GCSConnectorOptions.GCS_VERTICA_KEY_ID,
      GCSSparkOptions.GCS_VERTICA_KEY_ID,
      GCSEnvVars.GCS_VERTICA_KEY_ID).sequence

    val secretAccessKeyOpt = getSensitiveOption(visibility)(
      config,
      GCSConnectorOptions.GCS_VERTICA_KEY_SECRET,
      GCSSparkOptions.GCS_VERTICA_KEY_SECRET,
      GCSEnvVars.GCS_VERTICA_KEY_SECRET).sequence

    (accessKeyIdOpt, secretAccessKeyOpt) match {
      case (Some(accessKeyId), Some(secretAccessKey)) => (accessKeyId, secretAccessKey).mapN(GCSVerticaAuth).map(Some(_))
      case (None, None) => None.validNec
      case (None, _) => MissingGCSVerticaKeyId().invalidNec
      case (_, None) => MissingGCSVerticaKeySecret().invalidNec
    }
  }

  def getAWSRegion(config: Map[String, String]): ValidationResult[Option[SensitiveArg[String]]] = {
    val visibility = Visible
    getFromConnectorOptions(visibility)(
      config,
      "aws_region",
      _ => getFromEnvVar(visibility)("AWS_DEFAULT_REGION"))
  }

  def getAWSSessionToken(config: Map[String, String]): ValidationResult[Option[SensitiveArg[String]]] = {
    getSensitiveOption(Secret)(
      config,
      "aws_session_token",
      "spark.hadoop.fs.s3a.session.token",
      "AWS_SESSION_TOKEN")
  }

  def getAWSCredentialsProvider(config: Map[String, String]): ValidationResult[Option[SensitiveArg[String]]] = {
    val visibility = Visible
    getFromConnectorOptions(visibility)(
      config,
      "aws_credentials_provider",
      _ => getFromSparkConfigOptions(visibility)(
        "spark.hadoop.fs.s3a.aws.credentials.provider", _ => None.validNec))
  }

  def getAWSEndpoint(config: Map[String, String]): ValidationResult[Option[SensitiveArg[String]]] = {
    val visibility = Visible
    getFromConnectorOptions(visibility)(
      config,
      "aws_endpoint",
      _ => getFromSparkConfigOptions(visibility)(
        "spark.hadoop.fs.s3a.endpoint", _ => None.validNec))
  }

  def getAWSSSLEnabled(config: Map[String, String]): ValidationResult[Option[SensitiveArg[String]]] = {
    val visibility = Visible
    getFromConnectorOptions(visibility)(
      config,
      "aws_enable_ssl",
      _ => getFromSparkConfigOptions(visibility)(
        "fs.s3a.connection.ssl.enabled", _ => None.validNec))
  }

  def getAWSPathStyleEnabled(config: Map[String, String]): ValidationResult[Option[SensitiveArg[String]]] = {
    val visibility = Visible
    getFromConnectorOptions(visibility)(
      config,
      "aws_enable_path_style",
      _ => getFromSparkConfigOptions(visibility)(
        "fs.s3a.path.style.access", _ => None.validNec))
  }

  def getBackupServerNode(config: Map[String, String]): ValidationResult[Option[String]] = {
    config.get("backup_server_node") match {
      case Some(backUpServer) => Some(backUpServer).validNec
      case None => None.validNec
    }
  }

  private def getSensitiveOption(visibility: Visibility)(
                 config: Map[String, String],
                 connectorOption: String,
                 sparkConfigOption: String,
                 envVar: String
               ): ValidationResult[Option[SensitiveArg[String]]] = {
      getFromConnectorOptions(visibility)(
        config,
        connectorOption,
        _ => getFromSparkConfigOptions(visibility)(
          sparkConfigOption,
          _ => getFromEnvVar(visibility)(envVar)))
  }

  case class SensitiveOptionFound(option: ValidationResult[Option[SensitiveArg[String]]], optionName: String)

  /**
   * Get the option and the its connector option name.
   * */
  private def getSensitiveOptionWithName(visibility: Visibility)(
    config: Map[String, String],
    connectorOption: String,
    sparkConfigOption: String,
    envVar: String
  ): SensitiveOptionFound = {
    SensitiveOptionFound(
      getSensitiveOption(visibility)(config, connectorOption, sparkConfigOption, envVar),
      connectorOption
    )
  }

  private def getFromConnectorOptions(visibility: Visibility)(
    config: Map[String, String],
    connectorOption: String,
    next: Unit => ValidationResult[Option[SensitiveArg[String]]]): ValidationResult[Option[SensitiveArg[String]]] = {
    config.get(connectorOption) match {
      case Some(token) => Some(SensitiveArg(visibility, ConnectorOption, token)).validNec
      case None => next(())
    }
  }

  private def getFromSparkConfigOptions(visibility: Visibility)(
    sparkConfigOption: String,
    next: Unit => ValidationResult[Option[SensitiveArg[String]]]): ValidationResult[Option[SensitiveArg[String]]] = {
    SparkSession.getActiveSession match {
      case Some(session) =>
        val sparkConf = session.sparkContext.getConf
        Try(sparkConf.get(sparkConfigOption)).toOption match {
          case Some(token) => Some(SensitiveArg(visibility, SparkConf, token)).validNec
          case None => next(())
        }
      case None => LoadConfigMissingSparkSessionError().invalidNec
    }
  }

  private def getFromEnvVar(visibility: Visibility)(envVar: String):  ValidationResult[Option[SensitiveArg[String]]] = {
    sys.env.get(envVar).map(token => SensitiveArg(visibility, EnvVar, token)).validNec
  }

  def getKeyStorePath(config: Map[String, String]): ValidationResult[Option[String]] = {
    config.get("key_store_path").validNec
  }

  def getKeyStorePassword(config: Map[String, String]): ValidationResult[Option[String]] = {
    config.get("key_store_password").validNec
  }

  def getTrustStorePath(config: Map[String, String]): ValidationResult[Option[String]] = {
    config.get("trust_store_path").validNec
  }

  def getTrustStorePassword(config: Map[String, String]): ValidationResult[Option[String]] = {
    config.get("trust_store_password").validNec
  }

  def getTablename(config: Map[String, String]): Option[String] = {
    config.get("table")
  }

  def getQuery(config: Map[String, String]): Option[String] = {
    config.get("query") match {
      case None => None
        // Strip the ';' from the query to allow for queries ending with this
      case Some(value) => Some(value.stripSuffix(";"))
    }
  }

  def getDbSchema(config: Map[String, String]): Option[String] = {
    config.get("dbschema")
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

  def getFilePermissions(config: Map[String, String]): ValidationResult[ValidFilePermissions] = {
    config.get("file_permissions") match {
      case None => ValidFilePermissions("700") // Default to allowing user and group
      case Some(str) => ValidFilePermissions(str)
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

  def getMergeKey(config: Map[String, String]) : ValidationResult[Option[ValidColumnList]] = {
    config.get("merge_key") match {
      case None => None.validNec
      case Some(listStr) => ValidColumnList(listStr)
    }
  }

  def getPreventCleanup(config: Map[String, String]) : ValidationResult[Boolean] = {
    config.get("prevent_cleanup") match {
      case Some(str) =>
        str match {
          case "true" => true.validNec
          case "false" => false.validNec
          case _ => InvalidPreventCleanupOption().invalidNec
        }
      case None => false.validNec
    }
  }

  def getTimeOperations(config: Map[String, String]) : ValidationResult[Boolean] = {
    config.get("time_operations") match {
      case Some(str) =>
        str match {
          case "true" => true.validNec
          case _ => false.validNec
        }
      case None => true.validNec
    }
  }

  def getArrayLength(config: Map[String, String]) : ValidationResult[Long] = {
    Try {config.getOrElse("array_length","0").toLong} match {
      case Success(len) => if(len < 0) InvalidArrayLengthError().invalidNec else  len.validNec
      case Failure(_) =>  InvalidArrayLengthError().invalidNec
    }
  }

  def validateAndGetJDBCAuth(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[JdbcAuth] = {
    val user = DSConfigSetupUtils.getUser(config)
    val password = DSConfigSetupUtils.getPassword(config)

    val serviceName = getKerberosServiceName(config)
    val hostname = getKerberosHostname(config)
    val jaasConfig = getJaasConfigName(config)

    (user, password, serviceName, hostname, jaasConfig) match {
      case (Some(u), _, Some(s), Some(h), Some(j)) => KerberosAuth(u, s, h, j).validNec
      case (Some(u), Some(p), _, _, _) => BasicJdbcAuth(u, p).validNec
      case (None, _, _, _, _) => UserMissingError().invalidNec
      case (_, None, None, None, None) => PasswordMissingError().invalidNec
      case (_, _, _, _, _) => KerberosAuthMissingError().invalidNec
    }
  }

  def validateAndGetJDBCSSLConfig(config: Map[String, String]): ValidationResult[JDBCTLSConfig] = {
    (getTLS(config),
    getKeyStorePath(config),
    getKeyStorePassword(config),
    getTrustStorePath(config),
    getTrustStorePassword(config)).mapN(JDBCTLSConfig)
  }

  /**
   * Parses the config map for JDBC config params, collecting any errors.
   */
  def validateAndGetJDBCConfig(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[JDBCConfig] = {
    (DSConfigSetupUtils.getHost(config),
    DSConfigSetupUtils.getPort(config),
    DSConfigSetupUtils.getDb(config),
    DSConfigSetupUtils.validateAndGetJDBCAuth(config),
    DSConfigSetupUtils.validateAndGetJDBCSSLConfig(config),
    DSConfigSetupUtils.getBackupServerNode(config)).mapN(JDBCConfig)
  }

  def validateAndGetFilestoreConfig(config: Map[String, String], sessionId: String): DSConfigSetupUtils.ValidationResult[FileStoreConfig] = {
    val awsOptions = (DSConfigSetupUtils.getAWSAuth(config),
      DSConfigSetupUtils.getAWSRegion(config),
      DSConfigSetupUtils.getAWSSessionToken(config),
      DSConfigSetupUtils.getAWSCredentialsProvider(config),
      DSConfigSetupUtils.getAWSEndpoint(config),
      DSConfigSetupUtils.getAWSSSLEnabled(config),
      DSConfigSetupUtils.getAWSPathStyleEnabled(config)
      ).mapN(AWSOptions)

    val gcsOptions = (DSConfigSetupUtils.getVerticaGCSAuth(config),
      DSConfigSetupUtils.getGCSKeyFile(config),
      DSConfigSetupUtils.getGCSServiceAccountAuth(config))
      .mapN(GCSOptions)

    (DSConfigSetupUtils.getStagingFsUrl(config),
      sessionId.validNec,
      DSConfigSetupUtils.getPreventCleanup(config),
      awsOptions,
      gcsOptions
      ).mapN(FileStoreConfig)
  }

  def validateAndGetTableSource(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[TableSource] = {
    val name = DSConfigSetupUtils.getTablename(config)
    val schema = DSConfigSetupUtils.getDbSchema(config)
    val query = DSConfigSetupUtils.getQuery(config)

    (query, name) match {
      case (Some(q), _) => TableQuery(q, SessionId.getId).validNec
      case (None, Some(n)) => TableName(n, schema).validNec
      case (None, None) => TableAndQueryMissingError().invalidNec
    }
  }

  def validateAndGetFullTableName(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[TableName] = {
    val name = DSConfigSetupUtils.getTablename(config)
    val schema = DSConfigSetupUtils.getDbSchema(config)
    val query = DSConfigSetupUtils.getQuery(config)

    (query, name) match {
      case (_, Some(n)) => TableName(n, schema).validNec
      case (Some(_), None) => QuerySpecifiedOnWriteError().invalidNec
      case (None, None) => TablenameMissingError().invalidNec
    }
  }

}



/**
  * Implementation for parsing user option map and getting read config
  */
class DSReadConfigSetup(val pipeFactory: VerticaPipeFactoryInterface = VerticaPipeFactory, val sessionIdInterface: SessionIdInterface = SessionId) extends DSConfigSetupInterface[ReadConfig] {
  private val logger: Logger = LogProvider.getLogger(classOf[DSReadConfigSetup])

  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[ReadConfig]] or sequence of [[ConnectorError]]
    */
  override def validateAndGetConfig(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[ReadConfig] = {
    val sessionId = sessionIdInterface.getId

    val res = (
      DSConfigSetupUtils.validateAndGetJDBCConfig(config),
      DSConfigSetupUtils.validateAndGetFilestoreConfig(config, sessionId),
      DSConfigSetupUtils.validateAndGetTableSource(config),
      DSConfigSetupUtils.getPartitionCount(config),
      None.validNec,
      DSConfigSetupUtils.getFilePermissions(config),
      DSConfigSetupUtils.getMaxRowGroupSize(config),
      DSConfigSetupUtils.getMaxFileSize(config),
      DSConfigSetupUtils.getTimeOperations(config)
    ).mapN(DistributedFilesystemReadConfig).andThen { initialConfig =>
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

    // Check for options left over from old connector
    DSConfigSetupUtils.logOrAppendErrorsForOldConnectorOptions(config, res, logger)
  }

  /**
   * Calls read pipe implementation to perform initial setup for the read operation.
   *
   * @param config Configuration data for the read operation. Used to construct pipe for performing initial setup.
   * @return List of partitioning information for the operation to pass down to readers, or error that occured in setup.
   */
  override def performInitialSetup(config: ReadConfig): ConnectorResult[Option[PartitionInfo]] = {
    pipeFactory.getReadPipe(config).doPreReadSteps() match {
      case Right(partitionInfo) => Right(Some(partitionInfo))
      case Left(err) => Left(err)
    }
  }

  /**
   * Returns the schema of the table being read
   *
   * @param config Configuration data for the read operation. Contains the metadata required for returning the table schema.
   * @return The table schema or an error that occured trying to retrieve it
   */
  override def getTableSchema(config: ReadConfig): ConnectorResult[StructType] =  {
    config match {
      case DistributedFilesystemReadConfig(_, _, _, _, verticaMetadata, _, _, _, _) =>
        verticaMetadata match {
          case None => Left(SchemaDiscoveryError())
          case Some(metadata) => Right(metadata.schema)
        }
    }
  }
}

/**
  * Implementation for parsing user option map and getting write config
  */
class DSWriteConfigSetup(val schema: Option[StructType], val pipeFactory: VerticaPipeFactoryInterface = VerticaPipeFactory, sessionIdInterface: SessionIdInterface = SessionId) extends DSConfigSetupInterface[WriteConfig] {
  private val logger: Logger = LogProvider.getLogger(classOf[DSWriteConfigSetup])

  /**
    * Validates the user option map and parses read config
    *
    * @return Either [[WriteConfig]] or [[ConnectorError]]
    */
  override def validateAndGetConfig(config: Map[String, String]): DSConfigSetupUtils.ValidationResult[WriteConfig] = {
    val sessionId = sessionIdInterface.getId

    // List of configuration errors. We keep these all so that we report all issues with the given configuration to the user at once and they don't have to solve issues one by one.
    val res = schema match {
      case Some(passedInSchema) =>
        (
          DSConfigSetupUtils.validateAndGetJDBCConfig(config),
          DSConfigSetupUtils.validateAndGetFilestoreConfig(config, sessionId),
          DSConfigSetupUtils.validateAndGetFullTableName(config),
          passedInSchema.validNec,
          DSConfigSetupUtils.getStrLen(config),
          DSConfigSetupUtils.getTargetTableSQL(config),
          DSConfigSetupUtils.getCopyColumnList(config),
          sessionId.validNec,
          DSConfigSetupUtils.getFailedRowsPercentTolerance(config),
          DSConfigSetupUtils.getFilePermissions(config),
          DSConfigSetupUtils.getCreateExternalTable(config),
          DSConfigSetupUtils.getSaveJobStatusTable(config),
          DSConfigSetupUtils.getMergeKey(config),
          DSConfigSetupUtils.getTimeOperations(config),
          DSConfigSetupUtils.getArrayLength(config)
        ).mapN(DistributedFilesystemWriteConfig)
      case None =>
        MissingSchemaError().invalidNec
    }

    // Check for options left over from old connector
    DSConfigSetupUtils.logOrAppendErrorsForOldConnectorOptions(config, res, logger)
  }

  /**
   * Performs initial steps for write operation.
   *
   * @return None, partitioning info not needed for write operation.
   */
  override def performInitialSetup(config: WriteConfig): ConnectorResult[Option[PartitionInfo]] = {
    val pipe = pipeFactory.getWritePipe(config)
    pipe.doPreWriteSteps() match {
      case Left(err) => Left(err)
      case Right(_) => Right(None)
    }
  }

  /**
   * Returns the same schema that was passed in to this class.
   */
  override def getTableSchema(config: WriteConfig): ConnectorResult[StructType] = this.schema match {
    case Some(schema) => Right(schema)
    case None => Left(SchemaDiscoveryError())
  }
}
