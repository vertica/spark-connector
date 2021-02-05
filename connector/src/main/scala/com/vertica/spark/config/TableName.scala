package com.vertica.spark.config

final case class TableName(name: String, dbschema: Option[String]) {
  def getFullTableName : String = {
    dbschema match {
      case None => name
      case Some(schema) => schema + "." + name
    }
  }
}
