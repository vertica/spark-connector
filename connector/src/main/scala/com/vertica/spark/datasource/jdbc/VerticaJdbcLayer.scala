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

import com.vertica.spark.util.error.JdbcErrorType._
import com.vertica.spark.config.JDBCConfig

/**
  * Interface for communicating with a JDBC source
  */
trait JdbcLayerInterface {

  /**
    * Runs a query that should return a ResultSet
    */
  def query(query: String): Either[JDBCLayerError, ResultSet]

  def valid(): Either[JDBCLayerError, Unit]

  /**
    * Executes a statement
    *
    * Used for ddl or dml statements.
    */
  def execute(statement: String): Either[JDBCLayerError, Unit]

  /**
    * Close and cleanup
    */
  def close(): Unit
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
  private def getStatement: Either[JDBCLayerError, Statement] = {
    connection match {
      case Some(conn) =>
        if(conn.isValid(0))
        {
          val stmt = conn.createStatement()

          Right(stmt)
        }
        else {
          logger.error("Can't connect to Vertica: connection down.")
          Left(JDBCLayerError(ConnectionError))
        }
      case None =>
        logger.error("Can't connect to Vertica: initial connection failed.")
        Left(JDBCLayerError(ConnectionError))
    }
  }

  /**
    * Turns exception from driver into error and logs.
    */
  private def handleJDBCException(e: Throwable): JDBCLayerError = {
    e match {
      case ex: java.sql.SQLSyntaxErrorException =>
        logger.error("Syntax Error.", ex)
        JDBCLayerError(SyntaxError, ex.getMessage)
      case ex: java.sql.SQLDataException =>
        logger.error("Data Type Error.", ex)
        JDBCLayerError(DataTypeError, ex.getMessage)
      case _: Throwable =>
        logger.error("Unexpected SQL Error.", e)
        JDBCLayerError(GenericError, e.getMessage)
    }
  }

  /**
    * Runs a query against Vertica that should return a ResultSet
    */
  def query(query: String): Either[JDBCLayerError, ResultSet] = {
    logger.debug("Attempting to send query: " + query)
    getStatement match {
      case Right(stmt) =>
        try{
          val rs = stmt.executeQuery(query)
          Right(rs)
        }
        catch{
          case e: Throwable => Left(handleJDBCException(e))
        }
      case Left(err) => Left(err)
    }
  }

  /**
    * Executes a statement
    */
  def execute(statement: String): Either[JDBCLayerError, Unit]= {
    logger.debug("Attempting to execute statement: " + statement)
    getStatement match {
      case Right(stmt) =>
        try {
          stmt.execute(statement)
          Right()
        }
        catch{
          case e: Throwable => Left(handleJDBCException(e))
        }
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
}
