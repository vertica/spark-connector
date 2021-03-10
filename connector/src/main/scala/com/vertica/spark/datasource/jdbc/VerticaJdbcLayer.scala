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
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import java.util

import com.vertica.spark.util.error.JdbcErrorType._
import com.vertica.spark.config.JDBCConfig

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
  def query(query: String, params: Seq[JdbcLayerParam] = Seq()): Either[JDBCLayerError, ResultSet]

  /**
    * Executes a statement
    *
    * Used for ddl or dml statements.
    */
  def execute(statement: String, params: Seq[JdbcLayerParam] = Seq()): Either[JDBCLayerError, Unit]

  /**
   * Executes a statement returning a row count
   */
  def executeUpdate(statement: String, params: Seq[JdbcLayerParam] = Seq()): Either[JDBCLayerError, Int]

  /**
    * Close and cleanup
    */
  def close(): Unit


  /**
   * Converts and logs JDBC exception to our error format
   */
  def handleJDBCException(e: Throwable): JDBCLayerError

  /**
   * Commit transaction
   */
  def commit(): Either[JDBCLayerError, Unit]

  /**
   * Rollback transaction
   */
  def rollback(): Either[JDBCLayerError, Unit]
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

  private def getPreparedStatement(sql: String): Either[JDBCLayerError, PreparedStatement] = {
    connection match {
      case Some(conn) =>
        if(conn.isValid(0))
        {
          val stmt = conn.prepareStatement(sql)

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
  def handleJDBCException(e: Throwable): JDBCLayerError = {
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

  private def addParamsToStatement(statement: PreparedStatement, params: Seq[JdbcLayerParam]): Unit = {
    var i = 0
    for(param <- params) {
      i += 1
      param match {
        case p: JdbcLayerStringParam => statement.setString(i, p.value)
        case p: JdbcLayerIntParam => statement.setInt(i, p.value)
      }
    }
  }

  /**
    * Runs a query against Vertica that should return a ResultSet
    */
  def query(query: String, params: Seq[JdbcLayerParam] = Seq()): Either[JDBCLayerError, ResultSet] = {
    logger.debug("Attempting to send query: " + query)
    try{
      getPreparedStatement(query) match {
        case Right(stmt) =>
          addParamsToStatement(stmt, params)
          val rs = stmt.executeQuery()
          Right(rs)
        case Left(err) => Left(err)
      }
    }
    catch{
      case e: Throwable => Left(handleJDBCException(e))
    }
  }


  /**
   * Executes a statement
    */
  def execute(statement: String, params: Seq[JdbcLayerParam] = Seq()): Either[JDBCLayerError, Unit]= {
    logger.debug("Attempting to execute statement: " + statement)
    try {
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
    }
    catch{
      case e: Throwable => Left(handleJDBCException(e))
    }
  }

  def executeUpdate(statement: String, params: Seq[JdbcLayerParam] = Seq()): Either[JDBCLayerError, Int] = {
    logger.debug("Attempting to execute statement: " + statement)
    if(params.nonEmpty) Left(JDBCLayerError(ParamsNotSupported))
    else {
      try {
        getStatement match {
          case Right(stmt) =>
            Right(stmt.executeUpdate(statement))
          case Left(err) => Left(err)
        }
      }
      catch{
        case e: Throwable => Left(handleJDBCException(e))
      }
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

  def commit(): Either[JDBCLayerError, Unit] = {
    logger.debug("Commiting.")
    connection match {
      case Some(conn) =>
        if(conn.isValid(0)){
          try {
            conn.commit()
            Right(())
          }
          catch {
            case e: Throwable => Left(handleJDBCException(e))
          }
        }
        else {
          Left(JDBCLayerError(ConnectionError))
        }
      case None =>
        Left(JDBCLayerError(ConnectionError))
    }
  }

  def rollback(): Either[JDBCLayerError, Unit] = {
    logger.debug("Commiting.")
    connection match {
      case Some(conn) =>
        if(conn.isValid(0)){
          try {
            conn.rollback()
            Right(())
          }
          catch {
            case e: Throwable => Left(handleJDBCException(e))
          }
        }
        else {
          Left(JDBCLayerError(ConnectionError))
        }
      case None =>
        Left(JDBCLayerError(ConnectionError))
    }
  }
}
