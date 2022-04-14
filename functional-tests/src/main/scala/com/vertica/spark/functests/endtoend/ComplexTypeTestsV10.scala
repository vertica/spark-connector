package com.vertica.spark.functests.endtoend

import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import com.vertica.spark.functests.TestUtils
import com.vertica.spark.util.error.ConnectorException
import com.vertica.spark.util.schema.MetadataKey
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}


class ComplexTypeTestsV10(readOpts: Map[String, String], writeOpts: Map[String, String], jdbcConfig: JDBCConfig, fileStoreConfig: FileStoreConfig)
  extends EndToEnd(readOpts, writeOpts, jdbcConfig, fileStoreConfig) {
  //  Todo: Add Complex Type functional tests for Vertica 10
}
