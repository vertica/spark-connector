package com.vertica.spark.config

import ch.qos.logback.classic.Level
import org.apache.spark.sql.types.StructType

trait WriteConfig extends GenericConfig

final case class DistributedFilesystemWriteConfig(override val logLevel: Level, jdbcConfig: JDBCConfig, fileStoreConfig: FileStoreConfig, tablename: TableName, schema: StructType, strlen: Long, targetTableSql: Option[String]) extends WriteConfig {
}

