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

import com.vertica.spark.config.JDBCConfig
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult

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
  private val logger = cfg.getLogger(classOf[VerticaJdbcLayer])

  private val prop = new util.Properties()
  prop.put("user", cfg.username)
  prop.put("password", cfg.password)

  // Load driver
  Class.forName("com.vertica.jdbc.Driver")

  private val jdbcURI = "jdbc:vertica://" + cfg.host + ":" + cfg.port + "/" + cfg.db
  logger.info("Connecting to Vertica with URI: " + jdbcURI)

  private val connection: ConnectorResult[Connection] = {
    Try { DriverManager.getConnection(jdbcURI, prop) }
      .toEither.left.map(handleConnectionException)
      .flatMap(conn =>
        this.useConnection(conn, c => {
          c.setAutoCommit(false)
          logger.info("Successfully connected to Vertica.")
          c
        }, handleConnectionException).left.map(_.context("Initial connection was not valid.")))
  }

  private def handleConnectionException(e: Throwable): ConnectorError = {
    e match {
      case e: java.sql.SQLException => ConnectionSqlError(e)
      case e: Throwable => ConnectionError(e)
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
      case ex: java.sql.SQLDataException => DataTypeError(ex)
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
