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

package com.vertica.spark.datasource.jdbc

import com.vertica.spark.util.error._
import java.sql.{DriverManager, PreparedStatement}
import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet
import java.util

import com.vertica.spark.config.{BasicJdbcAuth, JDBCConfig, KerberosAuth, LogProvider}
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.general.Utils
import buildinfo.BuildInfo
import org.apache.spark.sql.SparkSession

import scala.util.Try
import scala.util.Success
import scala.util.Failure

trait JdbcLayerParam

case class JdbcLayerStringParam(value: String) extends JdbcLayerParam
case class JdbcLayerIntParam(value: Int) extends JdbcLayerParam

/**
  * Interface for communicating with a JDBC source
  */
trait JdbcLayerInterface {

  /**
    * Runs a query that should return a ResultSet
    */
  def query(query: String, params: Seq[JdbcLayerParam] = Seq()): ConnectorResult[ResultSet]

  /**
    * Executes a statement
    *
    * Used for ddl or dml statements.
    */
  def execute(statement: String, params: Seq[JdbcLayerParam] = Seq()): ConnectorResult[Unit]

  /**
   * Executes a statement returning a row count
   */
  def executeUpdate(statement: String, params: Seq[JdbcLayerParam] = Seq()): ConnectorResult[Int]

  /**
    * Close and cleanup
    */
  def close(): ConnectorResult[Unit]

  /**
   * Converts and logs JDBC exception to our error format
   */
  def handleJDBCException(e: Throwable): ConnectorError

  /**
   * Commit transaction
   */
  def commit(): ConnectorResult[Unit]

  /**
   * Rollback transaction
   */
  def rollback(): ConnectorResult[Unit]

  /**
   * Configures the database session
   *
   * @param fileStoreLayer Interface to a filestore that provides an impersonation token via getImpersonationToken
   */
  def configureSession(fileStoreLayer: FileStoreLayerInterface): ConnectorResult[Unit]
}

object JdbcUtils {
  def tryJdbcToResult[T](jdbcLayer: JdbcLayerInterface, ttry: Try[T]): ConnectorResult[T] = {
    ttry match {
      case Success(value) => Right(value)
      case Failure(e) => Left(jdbcLayer.handleJDBCException(e))
    }
  }
}

/**
  * Implementation of layer for communicating with Vertica JDBC
  */
class VerticaJdbcLayer(cfg: JDBCConfig) extends JdbcLayerInterface {
  private val logger = LogProvider.getLogger(classOf[VerticaJdbcLayer])

  private val prop = new util.Properties()

  cfg.auth match {
    case BasicJdbcAuth(username, password) =>
      Utils.ignore(prop.put("user", username))
      Utils.ignore(prop.put("password", password))
    case KerberosAuth(username, kerberosServiceName, kerberosHostname, jaasConfigName) =>
      Utils.ignore(prop.put("user", username))
      Utils.ignore(prop.put("KerberosServiceName", kerberosServiceName))
      Utils.ignore(prop.put("KerberosHostname", kerberosHostname))
      Utils.ignore(prop.put("JAASConfigName", jaasConfigName))
  }

  addTLSProperties()

  // Load driver
  Class.forName("com.vertica.jdbc.Driver")

  private val jdbcURI = "jdbc:vertica://" + cfg.host + ":" + cfg.port + "/" + cfg.db
  logger.info("Connecting to Vertica with URI: " + jdbcURI)

  private lazy val connection: ConnectorResult[Connection] = {
    Try { DriverManager.getConnection(jdbcURI, prop) }
      .toEither.left.map(handleConnectionException)
      .flatMap(conn =>
        this.useConnection(conn, c => {
          c.setClientInfo("APPLICATIONNAME", this.createClientLabel)
          c.setAutoCommit(false)
          logger.info("Successfully connected to Vertica.")
          c
        }, handleConnectionException).left.map(_.context("Initial connection was not valid.")))
  }

  private def createClientLabel: String = {
    val authMethod = this.cfg.auth match {
      case BasicJdbcAuth(_, _) => "-p"
      case KerberosAuth(_, _, _, _) => "-k"
    }
    val sparkVersion = SparkSession.active.sparkContext.version
    "vspark" + "-vs" + BuildInfo.version + authMethod + "-sp" + sparkVersion
  }

  private def handleConnectionException(e: Throwable): ConnectorError = {
    e match {
      case e: java.sql.SQLException => ConnectionSqlError(e)
      case e: Throwable => ConnectionError(e)
    }
  }

  private def addTLSProperties(): Unit = {
    val tlsConfig = cfg.tlsConfig
    Utils.ignore(prop.put("TLSmode", tlsConfig.tlsMode.toString))
    tlsConfig.keyStorePath match {
      case Some(path) => Utils.ignore(prop.put("KeyStorePath", path))
      case None => ()
    }

    tlsConfig.keyStorePassword match {
      case Some(password) => Utils.ignore(prop.put("KeyStorePassword", password))
      case None => ()
    }

    tlsConfig.trustStorePath match {
      case Some(path) => Utils.ignore(prop.put("TrustStorePath", path))
      case None => ()
    }

    tlsConfig.trustStorePassword match {
      case Some(password) => Utils.ignore(prop.put("TrustStorePassword", password))
      case None => ()
    }
  }

  /**
    * Gets a statement object or connection error if something is wrong.
    */
  private def getStatement: ConnectorResult[Statement] = {
    this.connection.flatMap(conn => this.useConnection(conn, c => c.createStatement(), handleJDBCException)
      .left.map(_.context("getStatement: Error while trying to create statement.")))
  }

  private def getPreparedStatement(sql: String): ConnectorResult[PreparedStatement] = {
    this.connection.flatMap(conn => this.useConnection(conn, c => c.prepareStatement(sql), handleJDBCException)
      .left.map(_.context("getPreparedStatement: Error while getting prepared statement.")))
  }

  /**
    * Turns exception from driver into error and logs.
    */
  def handleJDBCException(e: Throwable): ConnectorError = {
    e match {
      case ex: java.sql.SQLSyntaxErrorException => SyntaxError(ex)
      case ex: java.sql.SQLDataException => DataError(ex)
      case _: Throwable => GenericError(e).context("Unexpected SQL Error.")
    }
  }

  private def addParamsToStatement(statement: PreparedStatement, params: Seq[JdbcLayerParam]): Unit = {
    params.zipWithIndex.foreach {
      case (param, idx) =>
        val i = idx + 1
        param match {
          case p: JdbcLayerStringParam => statement.setString(i, p.value)
          case p: JdbcLayerIntParam => statement.setInt(i, p.value)
        }
    }
  }

  /**
    * Runs a query against Vertica that should return a ResultSet
    */
  def query(query: String, params: Seq[JdbcLayerParam] = Seq()): ConnectorResult[ResultSet] = {
    logger.debug("Attempting to send query: " + query)
    Try{
      getPreparedStatement(query) match {
        case Right(stmt) =>
          addParamsToStatement(stmt, params)
          Right(stmt.executeQuery())
        case Left(err) => Left(err)
      }
    } match {
      case Success(v) => v
      case Failure(e) => Left(handleJDBCException(e).context("Error when sending query"))
    }
  }


  /**
   * Executes a statement
    */
  def execute(statement: String, params: Seq[JdbcLayerParam] = Seq()): ConnectorResult[Unit] = {
    logger.debug("Attempting to execute statement: " + statement)
    Try {
      if(params.nonEmpty) {
        getPreparedStatement(statement) match {
          case Right(stmt) =>
            addParamsToStatement(stmt, params)
            stmt.execute()
            Right()
          case Left(err) => Left(err)
        }
      }
      else {
        getStatement match {
          case Right(stmt) =>
            stmt.execute(statement)
            Right()
          case Left(err) => Left(err)
        }
      }
    } match {
      case Success(v) => v
      case Failure(e) => Left(handleJDBCException(e))
    }
  }

  def executeUpdate(statement: String, params: Seq[JdbcLayerParam] = Seq()): ConnectorResult[Int] = {
    logger.debug("Attempting to execute statement: " + statement)
    if(params.nonEmpty) {
      Left(ParamsNotSupported("executeUpdate"))
    }
    else {
      Try {
        getStatement.map(stmt => stmt.executeUpdate(statement))
      } match {
        case Success(v) => v
        case Failure(e) => Left(handleJDBCException(e))
      }
    }
  }

  /**
   * Closes the connection
   */
  def close(): ConnectorResult[Unit] = {
    logger.debug("Closing connection.")
    this.connection.flatMap(conn => this.useConnection(conn, c => c.close(), handleJDBCException)
      .left.map(_.context("close: JDBC Error closing the connection.")))
  }

  def commit(): ConnectorResult[Unit] = {
    logger.debug("Commiting.")
    this.connection.flatMap(conn => this.useConnection(conn, c => c.commit(), handleJDBCException)
      .left.map(_.context("commit: JDBC Error while commiting.")))
  }

  def rollback(): ConnectorResult[Unit] = {
    logger.debug("Rolling back.")
    this.connection.flatMap(conn => this.useConnection(conn, c => c.rollback(), handleJDBCException)
      .left.map(_.context("rollback: JDBC Error while rolling back.")))
  }

  def configureSession(fileStoreLayer: FileStoreLayerInterface): ConnectorResult[Unit] = {
    for {
      _ <- this.configureKerberosToFilestore(fileStoreLayer)
      _ <- this.configureAWSParameters(fileStoreLayer)
    } yield ()
  }

  private def configureAWSParameters(fileStoreLayer: FileStoreLayerInterface): ConnectorResult[Unit] = {
    val awsOptions = fileStoreLayer.getAWSOptions
    for {
      _ <- awsOptions.awsAuth match {
        case Some(awsAuth) =>
          val sql = s"ALTER SESSION SET AWSAuth='${awsAuth.accessKeyId.arg}:${awsAuth.secretAccessKey.arg}'"
          logger.info(s"Loaded AWS access key ID from ${awsAuth.accessKeyId.origin}")
          logger.info(s"Loaded AWS secret access key from ${awsAuth.secretAccessKey.origin}")
          this.execute(sql)
        case None =>
          logger.info("Did not set AWSAuth")
          Right(())
      }
      _ <- awsOptions.awsRegion match {
        case Some(awsRegion) =>
          val sql = s"ALTER SESSION SET AWSRegion='${awsRegion.arg}'"
          logger.info(s"Setting AWSRegion for session: $awsRegion")
          this.execute(sql)
        case None =>
          logger.info("Did not set AWSRegion")
          Right(())
      }
      _ <- awsOptions.awsSessionToken match {
        case Some(token) =>
          val sql = s"ALTER SESSION SET AWSSessionToken='${token.arg}'"
          logger.info(s"Loaded AWSSessionToken from ${token.origin}")
          this.execute(sql)
        case None =>
          logger.info("Did not set AWSSessionToken")
          Right(())
      }
      _ <- awsOptions.awsEndpoint match {
        case Some(endpoint) =>
          val sql = s"ALTER SESSION SET AWSEndpoint='${endpoint.arg}'"
          logger.info(s"Loaded AWSEndpoint from ${endpoint.origin}")
          this.execute(sql)
        case None =>
          logger.info("Did not set AWSEndpoint")
          Right(())
      }
    } yield ()
  }

  private def configureKerberosToFilestore(fileStoreLayer: FileStoreLayerInterface): ConnectorResult[Unit] = {
    SparkSession.getActiveSession match {
      case Some(session) =>
        logger.debug("Hadoop impersonation: found session")
        val hadoopConf = session.sparkContext.hadoopConfiguration
        val authMethod = Option(hadoopConf.get("hadoop.security.authentication"))
        logger.debug("Hadoop impersonation: auth method: " + authMethod)
        authMethod match {
          case Some(authMethod) if authMethod == "kerberos" =>
            for {
              nameNodeAddress <- Option(hadoopConf.get("dfs.namenode.https-address"))
                .orElse(Option(hadoopConf.get("dfs.namenode.http-address")))
                .toRight(MissingNameNodeAddressError())
              _ = logger.debug("Hadoop impersonation: name node address: " + nameNodeAddress)
              encodedDelegationToken <- fileStoreLayer.getImpersonationToken(cfg.auth.user)
              jsonString = s"""
                {
                   "authority": "$nameNodeAddress",
                   "token": "$encodedDelegationToken"
                }"""
              sql = s"ALTER SESSION SET HadoopImpersonationConfig='[$jsonString]'"
              _ <- this.execute(sql)
            } yield ()
          case _ =>
            logger.info("Kerberos is not enabled in the hadoop config.")
            Right(())
        }
      case None => Left(NoSparkSessionFound())
    }
  }

  private def useConnection[T](
                                connection: Connection,
                                action: Connection => T,
                                exceptionCatcher: Throwable => ConnectorError): ConnectorResult[T] = {
    try {
      if (connection.isValid(0)) {
        Right(action(connection))
      } else {
        Left(ConnectionDownError())
      }
    } catch {
      case e: Throwable => Left(exceptionCatcher(e))
    }
  }
}
