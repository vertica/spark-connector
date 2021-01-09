package com.vertica.spark.datasource.core

import com.typesafe.scalalogging.Logger
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import com.vertica.spark.config._
import com.vertica.spark.jdbc._
import com.vertica.spark.util.schema.SchemaTools

/**
  * Implementation of the pipe to Vertica using a distributed filesystem as an intermediary layer.
  *
  * Dependencies such as the JDBCLayerInterface may be optionally passed in, this option is in place mostly for tests. If not passed in, they will be instatitated here.
  */
class VerticaDistributedFilesystemReadPipe(val config: DistributedFilesystemReadConfig, val jdbcLayerInsert: Option[JdbcLayerInterface] = None) extends VerticaPipeInterface with VerticaPipeReadInterface {
  val logger: Logger = config.getLogger(classOf[VerticaDistributedFilesystemReadPipe])

  val jdbcLayer: JdbcLayerInterface = jdbcLayerInsert match {
      case Some(layer) =>
        layer
      case None =>
        new VerticaJdbcLayer(config.jdbcConfig)
  }

  private def retrieveMetadata(): Either[ConnectorError, VerticaMetadata] = {
    SchemaTools.readSchema(jdbcLayer, config.tablename) match {
      case Right(schema) => Right(VerticaMetadata(schema))
      case Left(errList) =>
        for(err <- errList) logger.error(err.msg)
        Left(ConnectorError(SchemaDiscoveryError))
    }
  }

  /**
    * Gets metadata, either cached in configuration object or retrieved from Vertica if we haven't yet.
    */
  def getMetadata(): Either[ConnectorError, VerticaMetadata] = {
    config.metadata match {
      case Some(data) => Right(data)
      case None => retrieveMetadata()
    }
  }

  /**
    * Returns the default number of rows to read/write from this pipe at a time.
    */
  def getDataBlockSize(): Either[ConnectorError, Long] = Right(1)

  /**
    * Initial setup for the whole read operation. Called by driver.
    */
  def doPreReadSteps(): Either[ConnectorError, Unit] = ???

  /**
    * Initial setup for the read of an individual partition. Called by executor.
    */
  def startPartitionRead(): Either[ConnectorError, Unit] = ???


  /**
    * Reads a block of data to the underlying source. Called by executor.
    */
  def readData: Either[ConnectorError, DataBlock] = ???


  /**
    * Ends the read, doing any necessary cleanup. Called by executor once reading the partition is done.
    */
  def endPartitionRead(): Either[ConnectorError, Unit] = ???

}

