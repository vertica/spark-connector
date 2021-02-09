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

package com.vertica.spark.datasource.core

import com.typesafe.scalalogging.Logger
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import com.vertica.spark.config._
import com.vertica.spark.datasource.jdbc._
import cats.implicits._
import com.vertica.spark.util.schema.{SchemaTools, SchemaToolsInterface}
import com.vertica.spark.datasource.fs._
import com.vertica.spark.util.cleanup.{CleanupUtils, CleanupUtilsInterface, FileCleanupInfo}

/**
 * Represents a portion of a parquet file
 *
 * @param filename Full path with name of the parquet file
 * @param minRowGroup First row group to read from parquet file
 * @param maxRowGroup Lasst row group to read from parquet file
 * @param rangeIdx Range index for this file. Used to track access to this file / cleanup among different nodes. If there are three ranges for a given file this will be a value between 0 and 2
 */
final case class ParquetFileRange(filename: String, minRowGroup: Int, maxRowGroup: Int, rangeIdx: Option[Int] = None)

/**
 * Partition for distributed filesystem transport method using parquet files
 *
 * @param fileRanges List of files and ranges of row groups to read for those files
 * @param rangeCountMap Map representing how many file ranges exist for each file. Used for tracking and cleanup.
 */
final case class VerticaDistributedFilesystemPartition(fileRanges: Seq[ParquetFileRange], rangeCountMap: Option[Map[String, Int]] = None) extends VerticaPartition


/**
  * Implementation of the pipe to Vertica using a distributed filesystem as an intermediary layer.
  *
  * Dependencies such as the JDBCLayerInterface may be optionally passed in, this option is in place mostly for tests. If not passed in, they will be instatitated here.
  */
class VerticaDistributedFilesystemReadPipe(val config: DistributedFilesystemReadConfig, val fileStoreLayer: FileStoreLayerInterface, val jdbcLayer: JdbcLayerInterface, val schemaTools: SchemaToolsInterface, val cleanupUtils: CleanupUtilsInterface = CleanupUtils, val dataSize: Int = 1)  extends VerticaPipeInterface with VerticaPipeReadInterface {
  private val logger: Logger = config.getLogger(classOf[VerticaDistributedFilesystemReadPipe])

  private def retrieveMetadata(): Either[ConnectorError, VerticaMetadata] = {
    schemaTools.readSchema(this.jdbcLayer, this.config.tablename.getFullTableName) match {
      case Right(schema) => Right(VerticaReadMetadata(schema))
      case Left(errList) =>
        for(err <- errList) logger.error(err.msg)
        Left(ConnectorError(SchemaDiscoveryError))
    }
  }

  /**
    * Gets metadata, either cached in configuration object or retrieved from Vertica if we haven't yet.
    */
  override def getMetadata: Either[ConnectorError, VerticaMetadata] = {
    this.config.metadata match {
      case Some(data) => Right(data)
      case None => this.retrieveMetadata()
    }
  }

  /**
    * Returns the default number of rows to read/write from this pipe at a time.
    */
  override def getDataBlockSize: Either[ConnectorError, Long] = Right(dataSize)

  /**
   * Increments a count for a given file and returns an index (count - 1)
   */
  private def incrementRangeMapGetIndex(map: scala.collection.mutable.Map[String, Int], filename: String) : Int = {
    if(!map.contains(filename)){
      map(filename) = 1
    }
    else {
      map(filename) += 1
    }
    map(filename) - 1
  }

  private def getPartitionInfo(fileMetadata: Seq[ParquetFileMetadata], rowGroupRoom: Int): Either[ConnectorError, PartitionInfo] = {
    // Now, create partitions splitting up files roughly evenly
    var i = 0
    var partitions = List[VerticaDistributedFilesystemPartition]()
    var curFileRanges = List[ParquetFileRange]()
    val rangeCountMap = scala.collection.mutable.Map[String, Int]()

    for(m <- fileMetadata) {
      val size = m.rowGroupCount
      var j = 0
      var low = 0
      while(j < size){
        if(i == rowGroupRoom-1){ // Reached end of partition, cut off here
          val rangeIdx = incrementRangeMapGetIndex(rangeCountMap, m.filename)
          val frange = ParquetFileRange(m.filename, low, j, Some(rangeIdx))
          curFileRanges = curFileRanges :+ frange
          val partition = VerticaDistributedFilesystemPartition(curFileRanges)
          partitions = partitions :+ partition
          curFileRanges = List[ParquetFileRange]()
          i = 0
          low = j+1
        }
        else if(j == size - 1){ // Reached end of file's row groups, add to file ranges
          val rangeIdx = incrementRangeMapGetIndex(rangeCountMap, m.filename)
          val frange = ParquetFileRange(m.filename, low, j, Some(rangeIdx))
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
    if(curFileRanges.nonEmpty) {
      val partition = VerticaDistributedFilesystemPartition(curFileRanges)
      partitions = partitions :+ partition
    }

    // Add range count map info to partition
    partitions = partitions.map(part => part.copy(rangeCountMap = Some(rangeCountMap.toMap)))

    Right(
      PartitionInfo(partitions.toArray)
    )
  }
  /**
    * Initial setup for the whole read operation. Called by driver.
    */
  override def doPreReadSteps(): Either[ConnectorError, PartitionInfo] = {
    getMetadata match {
      case Left(err) => return Left(err)
      case Right(_) => ()
    }

    val fileStoreConfig = config.fileStoreConfig

    // Create unique directory for session
    fileStoreLayer.createDir(fileStoreConfig.address) match {
      case Left(err) =>
        err.err match {
          case CreateDirectoryAlreadyExistsError =>
            logger.debug("Directory already existed: " + fileStoreConfig.address)
          case _ =>
            logger.error("Failed to create directory: " + fileStoreConfig.address)
            return Left(err)
        }
      case Right(_) =>
    }

    val delimiter = if(fileStoreConfig.address.takeRight(1) == "/" || fileStoreConfig.address.takeRight(1) == "\\") "" else "/"
    val hdfsPath = fileStoreConfig.address + delimiter + config.tablename.getFullTableName

    // Remove export directory if it exists (Vertica must create this dir)
    fileStoreLayer.removeDir(hdfsPath) match {
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

    def castToVarchar: String => String = colName => colName + "::varchar AS " + colName

    val cols: String = schemaTools.getColumnInfo(jdbcLayer, config.tablename.getFullTableName) match {
      case Left(err) =>
        logger.error(err.msg)
        return Left(ConnectorError(CastingSchemaReadError))
      case Right(columnDefs) => columnDefs.map(info => {
        info.colType match {
          case java.sql.Types.OTHER =>
            val typenameNormalized = info.colTypeName.toLowerCase()
            if (typenameNormalized.startsWith("interval") ||
              typenameNormalized.startsWith("uuid"))
              castToVarchar(info.label)
            else
              info.label
          case java.sql.Types.TIME => castToVarchar(info.label)
          case _ => info.label
        }
      }).mkString(",")
    }

    val exportStatement = "EXPORT TO PARQUET(directory = '" + hdfsPath + "', fileSizeMB = " + maxFileSize + ", rowGroupSizeMB = " + maxRowGroupSize + ", fileMode = '" + filePermissions + "', dirMode = '" + filePermissions  + "') AS " +
      "SELECT " + cols + " FROM " + config.tablename.getFullTableName + ";"

    jdbcLayer.execute(exportStatement) match {
      case Right(_) =>
      case Left(err) =>
        logger.error(err.msg)
        cleanupUtils.cleanupAll(fileStoreLayer, hdfsPath)
        return Left(ConnectorError(ExportFromVerticaError))
    }

    // Retrieve all parquet files created by Vertica
    val ret = fileStoreLayer.getFileList(hdfsPath) match {
      case Left(err) => Left(err)
      case Right(fileList) =>
        if(fileList.isEmpty){
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
          } yield partitionInfo
        }
    }

    // If there's an error, cleanup
    ret match {
      case Left(_) => cleanupUtils.cleanupAll(fileStoreLayer, hdfsPath)
      case Right(_) => ()
    }
    ret
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

    // Check if empty and initialize with first file range
    part.fileRanges.headOption match {
      case None =>
        logger.warn("No files to read set on partition.")
        Left(ConnectorError(DoneReading))
      case Some(head) =>
        fileStoreLayer.openReadParquetFile(head)
    }
  }

  private def getCleanupInfo(part: VerticaDistributedFilesystemPartition, curIdx: Int): Option[FileCleanupInfo] = {
    if(curIdx >= part.fileRanges.size) {
      logger.warn("Invalid fileIdx " + this.fileIdx + ", can't perform cleanup.")
      return None
    }

    val curRange = part.fileRanges(curIdx)
    part.rangeCountMap match {
      case Some(rangeCountMap) if rangeCountMap.contains(curRange.filename) => curRange.rangeIdx match {
        case Some(rangeIdx) => Some(FileCleanupInfo(curRange.filename,rangeIdx,rangeCountMap(curRange.filename)))
        case None =>
          logger.warn("Missing range count index. Not performing any cleanup for file " + curRange.filename)
          None
      }
      case None =>
        logger.warn("Missing range count map. Not performing any cleanup for file " + curRange.filename)
        None
      case _ =>
        logger.warn("Missing value in range count map. Not performing any cleanup for file " + curRange.filename)
        None
    }
  }

  /**
    * Reads a block of data to the underlying source. Called by executor.
    */
  def readData: Either[ConnectorError, DataBlock] = {
    val part = this.partition match {
      case None => return Left(ConnectorError(UninitializedReadError))
      case Some(p) => p
    }
    val ret = fileStoreLayer.readDataFromParquetFile(dataSize) match {
      case Left(err) => err.err match {
        case DoneReading =>

          // Cleanup old file if required
          getCleanupInfo(part,this.fileIdx) match {
            case Some(cleanupInfo) => cleanupUtils.checkAndCleanup(fileStoreLayer, cleanupInfo) match {
              case Left(err) => logger.warn("Ran into error when calling cleaning up. Treating as non-fatal. Err: " + err.msg)
              case Right(_) => ()
            }
            case None => ()
          }

          // Next file
          this.fileIdx += 1
          if(this.fileIdx >= part.fileRanges.size) return Left(ConnectorError(DoneReading))

          for {
            _ <- fileStoreLayer.closeReadParquetFile()
            _ <- fileStoreLayer.openReadParquetFile(part.fileRanges(this.fileIdx))
            data <- fileStoreLayer.readDataFromParquetFile(dataSize)
            } yield data
        case _ => Left(err)
      }
      case Right(data) => Right(data)
    }

    // If there was an underlying error, call cleanup
    (ret, getCleanupInfo(part,this.fileIdx)) match {
      case (Left(_), Some(cleanupInfo)) => cleanupUtils.checkAndCleanup(fileStoreLayer, cleanupInfo)
      case _ => ()
    }
    ret
  }


/**
  * Ends the read, doing any necessary cleanup. Called by executor once reading the partition is done.
  */
  def endPartitionRead(): Either[ConnectorError, Unit] = fileStoreLayer.closeReadParquetFile()

}

