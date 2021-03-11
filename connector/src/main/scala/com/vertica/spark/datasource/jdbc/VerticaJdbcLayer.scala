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
import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet
import java.util
import java.sql.DriverManager

import com.vertica.spark.config.JDBCConfig
import com.vertica.spark.util.error.ErrorHandling.{ConnectorResult, JdbcResult}

import scala.util.Try

/**
  * Interface for communicating with a JDBC source
  */
trait JdbcLayerInterface {

  /**
    * Runs a query that should return a ResultSet
    */
  def query(query: String): ConnectorResult[ResultSet]

  /**
    * Executes a statement
    *
    * Used for ddl or dml statements.
    */
  def execute(statement: String): ConnectorResult[Unit]

  /**
   * Executes a statement returning a row count
   */
  def executeUpdate(statement: String): ConnectorResult[Int]

  /**
    * Close and cleanup
    */
  def close(): Unit


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

  var connection: Option[Connection] = None
  try{
    val conn = DriverManager.getConnection(jdbcURI, prop)
    if(conn.isValid(0))
    {
      conn.setAutoCommit(false)
      logger.info("Successfully connected to Vertica.")
      connection = Some(conn)
    }
  }
  catch {
    case e: java.sql.SQLException => logger.error("SQL Exception when trying to connect to Vertica.", e)
    case e: Throwable => logger.error("Unexpected Exception when trying to connect to Vertica.", e)
  }

  /**
    * Gets a statement object or connection error if something is wrong.
    */
  private def getStatement: JdbcResult[Statement] = {
    connection match {
      case Some(conn) =>
        if (conn.isValid(0)) {
          val stmt = conn.createStatement()
          Right(stmt)
        } else {
          Left(ConnectionDownError())
        }
      case None =>
        Left(ConnectionError())
    }
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

  /**
    * Runs a query against Vertica that should return a ResultSet
    */
  def query(query: String): ConnectorResult[ResultSet] = {
    logger.debug("Attempting to send query: " + query)
    getStatement match {
      case Right(stmt) => Try { stmt.executeQuery(query) }.toEither.left.map(e => handleJDBCException(e)
        .context("Error when sending query"))
      case Left(err) => Left(err)
    }
  }

  /**
    * Executes a statement
    */
  def execute(statement: String): ConnectorResult[Unit] = {
    logger.debug("Attempting to execute statement: " + statement)
    getStatement match {
      case Right(stmt) => Try { stmt.execute(statement); () }.toEither.left.map(e => handleJDBCException(e)
        .context("Error when executing query"))
      case Left(err) => Left(err)
    }
  }

  def executeUpdate(statement: String): ConnectorResult[Int] = {
    logger.debug("Attempting to execute statement: " + statement)
    getStatement match {
      case Right(stmt) => Try { stmt.executeUpdate(statement) }.toEither.left.map(e => handleJDBCException(e)
        .context("Error when executing update"))
      case Left(err) => Left(err)
    }
  }

  /**
    * Closes the connection
    */
  def close(): Unit = {
    logger.debug("Closing connection.")
    connection match {
      case Some(conn) =>
        if(conn.isValid(0)){
          conn.close()
        }
      case None => ()
    }
  }

  def commit(): ConnectorResult[Unit] = {
    logger.debug("Commiting.")
    this.useConnection(conn => conn.commit())
      .left.map(err => err.context("Error getting connection while commiting"))
  }

  def rollback(): ConnectorResult[Unit] = {
    logger.debug("Rolling back.")
    this.useConnection(conn => conn.rollback())
      .left.map(err => err.context("Error getting connection while rolling back"))
  }

  private def useConnection(action: Connection => Unit): ConnectorResult[Unit] = {
    connection match {
      case Some(conn) =>
        if (conn.isValid(0)){
          Try { action(conn) }.toEither.left.map(e => handleJDBCException(e))
        } else {
          Left(ConnectionDownError())
        }
      case None => Left(ConnectionError())
    }
  }
}
