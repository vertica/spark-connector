package com.vertica.spark.jdbc

import com.vertica.spark.config._
import com.vertica.spark.util.error._
import com.vertica.jdbc.VerticaCopyStream;
import java.sql.ResultSet

/**
  * Interface for communicating with a JDBC source
  */
trait VerticaJdbcLayerInterface {
/**
  * Runs a query that should return a ResultSet
  */
  def query(query: String): Either[JDBCLayerError, ResultSet]


/**
  * Executes a statement
  *
  * Used for ddl or dml statements.
  */
  def execute(statement: String): Boolean
}
