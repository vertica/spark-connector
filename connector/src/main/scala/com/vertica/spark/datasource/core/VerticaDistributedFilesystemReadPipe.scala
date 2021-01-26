package com.vertica.spark.datasource.core

import com.typesafe.scalalogging.Logger
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import com.vertica.spark.config._
import com.vertica.spark.jdbc._
import com.vertica.spark.util.schema.SchemaTools
import com.vertica.spark.datasource.fs._
import cats.implicits._
import org.apache.hadoop.conf.Configuration
import com.vertica.spark.util.schema.{SchemaTools, SchemaToolsInterface}
import com.vertica.spark.datasource.fs._
import org.apache.spark.sql.connector.read.InputPartition

final case class ParquetFileRange(filename: String, minRowGroup: Int, maxRowGroup: Int)

final case class VerticaDistributedFilesystemPartition(fileRanges: Seq[ParquetFileRange]) extends VerticaPartition

/**
  * Implementation of the pipe to Vertica using a distributed filesystem as an intermediary layer.
  *
  * Dependencies such as the JDBCLayerInterface may be optionally passed in, this option is in place mostly for tests. If not passed in, they will be instatitated here.
  */
class VerticaDistributedFilesystemReadPipe(val config: DistributedFilesystemReadConfig, val fileStoreLayer: FileStoreLayerInterface, val jdbcLayer: JdbcLayerInterface, val schemaTools: SchemaToolsInterface) extends VerticaPipeInterface with VerticaPipeReadInterface {
  val logger: Logger = config.getLogger(classOf[VerticaDistributedFilesystemReadPipe])
  var dataSize = 1

  private def retrieveMetadata(): Either[ConnectorError, VerticaMetadata] = {
    schemaTools.readSchema(this.jdbcLayer, this.config.tablename.getFullTableName) match {
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
  override def getDataBlockSize(): Either[ConnectorError, Long] = Right(dataSize)


  private def getPartitionInfo(fileMetadata: Seq[ParquetFileMetadata], rowGroupRoom: Int): Either[ConnectorError, PartitionInfo] = {
    // Now, create partitions splitting up files roughly evenly
    var i = 0
    var partitions = List[VerticaDistributedFilesystemPartition]()
    var curFileRanges = List[ParquetFileRange]()
    for(m <- fileMetadata) {
      val size = m.rowGroupCount
      var j = 0
      var low = 0
      while(j < size){
        if(i == rowGroupRoom-1) { // Reached end of partition, cut off here
          val frange = ParquetFileRange(m.filename, low, j)
          curFileRanges = curFileRanges :+ frange
          val partition = VerticaDistributedFilesystemPartition(curFileRanges)
          partitions = partitions :+ partition
          curFileRanges = List[ParquetFileRange]()
          i = 0
          low = j+1
        }
        else if(j == size - 1) { // Reached end of file's row groups, add to file ranges
          val frange = ParquetFileRange(m.filename, low, j)
          curFileRanges = curFileRanges :+ frange
          i += 1
        }
        else {
          i += 1
        }

        j += 1
      }
    }
    // Last partition if leftover (only partition not of rowGroupRoom size)
    if(!curFileRanges.isEmpty) {
      val partition = VerticaDistributedFilesystemPartition(curFileRanges)
      partitions = partitions :+ partition
    }

    Right(
      PartitionInfo(partitions.toArray)
    )
  }
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
    val hdfsPath = fileStoreConfig.address + delimiter + config.tablename.getFullTableName

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

    val exportStatement = "EXPORT TO PARQUET(directory = '" + hdfsPath + "', fileSizeMB = " + maxFileSize + ", rowGroupSizeMB = " + maxRowGroupSize + ", fileMode = '" + filePermissions + "', dirMode = '" + filePermissions  + "') AS SELECT * FROM " + config.tablename.getFullTableName + ";"
    jdbcLayer.execute(exportStatement) match {
      case Right(_) =>
      case Left(err) =>
        logger.error(err.msg)
        return Left(ConnectorError(ExportFromVerticaError))
    }

    // Retrieve all parquet files created by Vertica
    fileStoreLayer.getFileList(hdfsPath) match {
      case Left(err) => Left(err)
      case Right(fileList) =>
        if(fileList.isEmpty) {
          logger.error("Returned file list was empty, so cannot create valid partition info")
          Left(ConnectorError(PartitioningError))
        }
        else {
          val partitionCount = config.partitionCount match {
            case Some(count) => count
            case None => fileList.size // Default to 1 partition / file
          }

          for {
            fileMetadata <- fileList.map(filename => fileStoreLayer.getParquetFileMetadata(filename)).toList.sequence
            // Get room per partition for row groups in order to split the groups up evenly
            rowGroupRoom <- {
              val totalRowGroups = fileMetadata.map(_.rowGroupCount).sum
              if(totalRowGroups < partitionCount) {
                Left(ConnectorError(PartitioningError))
              } else  {
                val extraSpace = if(totalRowGroups % partitionCount == 0) 0 else 1
                Right((totalRowGroups / partitionCount) + extraSpace)
              }
            }
            partitionInfo <- getPartitionInfo(fileMetadata, rowGroupRoom)
          } yield (partitionInfo)
        }
    }
  }

  var partition : Option[VerticaDistributedFilesystemPartition] = None
  var fileIdx = 0

  /**
    * Initial setup for the read of an individual partition. Called by executor.
    */
  def startPartitionRead(verticaPartition: VerticaPartition): Either[ConnectorError, Unit] = {
    val part = verticaPartition match {
      case p: VerticaDistributedFilesystemPartition => p
      case _ => return Left(ConnectorError(InvalidPartition))
    }
    this.partition = Some(part)
    this.fileIdx = 0

    if(part.fileRanges.isEmpty) {
      logger.warn("No files to read set on partition.")
      return Left(ConnectorError(DoneReading))
    }

    fileStoreLayer.openReadParquetFile(part.fileRanges.head)
  }


  /**
    * Reads a block of data to the underlying source. Called by executor.
    */
  def readData: Either[ConnectorError, DataBlock] = {
    val part = this.partition match {
      case None => return Left(ConnectorError(UninitializedReadError))
      case Some(p) => p
    }
    fileStoreLayer.readDataFromParquetFile(dataSize) match {
      case Left(err) => err.err match {
        case DoneReading =>
          this.fileIdx += 1
          if(this.fileIdx >= part.fileRanges.size) return Left(ConnectorError(DoneReading))

          for {
            _ <- fileStoreLayer.closeReadParquetFile()
            _ <- fileStoreLayer.openReadParquetFile(part.fileRanges(this.fileIdx))
            data <- fileStoreLayer.readDataFromParquetFile(dataSize)
            } yield (data)
        case _ => return Left(err)
      }
      case Right(data) => Right(data)
    }
  }


  /**
    * Ends the read, doing any necessary cleanup. Called by executor once reading the partition is done.
    */
  def endPartitionRead(): Either[ConnectorError, Unit] = fileStoreLayer.closeReadParquetFile()

}

