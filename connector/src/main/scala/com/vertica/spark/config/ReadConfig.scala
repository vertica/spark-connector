package com.vertica.spark.config

import ch.qos.logback.classic.Level

trait ReadConfig extends GenericConfig

final case class TableName(name: String, dbschema: Option[String]) {
  def getFullTableName : String = {
    dbschema match {
      case None => name
      case Some(schema) => schema + "." + name
    }
  }
}

final case class DistributedFilesystemReadConfig(override val logLevel: Level, jdbcConfig: JDBCConfig, fileStoreConfig: FileStoreConfig, tablename: TableName, partitionCount: Option[Int], metadata: Option[VerticaMetadata]) extends ReadConfig
