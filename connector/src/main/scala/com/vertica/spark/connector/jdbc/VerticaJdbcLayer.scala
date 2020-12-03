package com.vertica.spark.jdbc

import com.vertica.spark.config._
import com.vertica.spark.util.error._
import com.vertica.jdbc.VerticaCopyStream;
import java.sql.ResultSet

trait VerticaJdbcLayerInterface {
  def Query(query : String) : Either[JDBCLayerError, ResultSet]
  def Execute(statement : String) : Boolean
  def GetCopyStream() : Either[JDBCLayerError,VerticaCopyStream] // Used for write to STDIN
}
