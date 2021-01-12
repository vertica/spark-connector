package com.vertica.spark.datasource.core

import com.typesafe.scalalogging.Logger
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import com.vertica.spark.config._
import com.vertica.spark.jdbc._
import com.vertica.spark.util.schema.SchemaTools
import com.vertica.spark.connector.fs._
import org.apache.hadoop.conf.Configuration
import com.vertica.spark.util.schema.{SchemaTools, SchemaToolsInterface}
import com.vertica.spark.connector.fs._
import org.apache.spark.sql.connector.read.InputPartition


/**
  * Implementation of the pipe to Vertica using a distributed filesystem as an intermediary layer.
  *
  * Dependencies such as the JDBCLayerInterface may be optionally passed in, this option is in place mostly for tests. If not passed in, they will be instatitated here.
  */
class VerticaDistributedFilesystemReadPipe(val config: DistributedFilesystemReadConfig, val fileStoreLayer: FileStoreLayerInterface, val jdbcLayer: JdbcLayerInterface, val schemaTools: SchemaToolsInterface) extends VerticaPipeInterface with VerticaPipeReadInterface {
  val logger: Logger = config.getLogger(classOf[VerticaDistributedFilesystemReadPipe])

  private def retrieveMetadata(): Either[ConnectorError, VerticaMetadata] = {
    schemaTools.readSchema(this.jdbcLayer, this.config.tablename) match {
      case Right(schema) => Right(VerticaMetadata(schema))
      case Left(errList) =>
        for(err <- errList) logger.error(err.msg)
        Left(ConnectorError(SchemaDiscoveryError))
    }
  }

  /**
    * Gets metadata, either cached in configuration object or retrieved from Vertica if we haven't yet.
    */
  override def getMetadata(): Either[ConnectorError, VerticaMetadata] = {
    this.config.metadata match {
      case Some(data) => Right(data)
      case None => this.retrieveMetadata()
    }
  }

  /**
    * Returns the default number of rows to read/write from this pipe at a time.
    */
  override def getDataBlockSize(): Either[ConnectorError, Long] = Right(1)

  /**
    * Initial setup for the whole read operation. Called by driver.
    */
  override def doPreReadSteps(): Either[ConnectorError, PartitionInfo] = {
    val hadoopConf : Configuration = new Configuration()
    val schema = getMetadata() match {
      case Left(err) => return Left(err)
      case Right(metadata) => metadata.schema
    }

    val fileStoreConfig = config.fileStoreConfig

    val delimiter = if(fileStoreConfig.address.takeRight(1) == "/" || fileStoreConfig.address.takeRight(1) == "\\") "" else "/"
    val hdfsPath = fileStoreConfig.address + delimiter + config.tablename

    fileStoreLayer.removeDir(hdfsPath) match {
      case Left(err) => return Left(err)
      case Right(_) =>
    }

    fileStoreLayer.createDir(hdfsPath) match {
      case Left(err) => return Left(err)
      case Right(_) =>
    }

    // File size params. The max size of a single file, and the max size of an individual row group inside the parquet file.
    // TODO: Tune these with performance tests. Determine whether a single value is suitable or if we need to add a user option.
    val maxFileSize = 512
    val maxRowGroupSize = 64

    // File permissions.
    // TODO: Add file permission option w/ default value '700'
    val filePermissions = "777"

    val exportStatement = "EXPORT TO PARQUET(directory = '" + hdfsPath + "', fileSizeMB = " + maxFileSize + ", rowGroupSizeMB = " + maxRowGroupSize + ", fileMode = '" + filePermissions + "', dirMode = '" + filePermissions  + "') AS SELECT * FROM " + config.tablename + ";"
    jdbcLayer.execute(exportStatement) match {
      case Right(_) =>
      case Left(err) =>
        logger.error(err.msg)
        return Left(ConnectorError(ExportFromVerticaError))
    }

    Right(PartitionInfo(Array[InputPartition]()))
  }

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

