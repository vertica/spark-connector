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
import com.vertica.spark.config._
import com.vertica.spark.datasource.jdbc._
import cats.implicits._
import com.vertica.spark.datasource.core.VerticaDistributedFilesystemPartitionTemplate.{VerticaDistributedFilesystemPartition, VerticaDistributedFilesystemPartitionIterator, makeVerticaDistribuedFSPartition, makeVerticaDistribuedFSPartitionIterator}
import com.vertica.spark.util.schema.{ColumnDef, SchemaTools, SchemaToolsInterface}
import com.vertica.spark.datasource.fs._
import com.vertica.spark.datasource.v2.PushdownFilter
import com.vertica.spark.util.cleanup.{CleanupUtils, CleanupUtilsInterface, FileCleanupInfo}
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.spark.sql.types.StructType

/**
 * Represents a portion of a parquet file
 *
 * @param filename Full path with name of the parquet file
 * @param minRowGroup First row group to read from parquet file
 * @param maxRowGroup Last row group to read from parquet file
 * @param rangeIdx Range index for this file. Used to track access to this file / cleanup among different nodes. If there are three ranges for a given file this will be a value between 0 and 2
 */
final case class ParquetFileRange(filename: String, minRowGroup: Int, maxRowGroup: Int, rangeIdx: Option[Int] = None)

case class ParquetFileRangeIterator(fileRanges: Iterator[ParquetFileRange]) {
  def next(): Option[ParquetFileRange] = {
    if (this.fileRanges.hasNext) {
      Some(this.fileRanges.next())
    } else {
      None
    }
  }
}

/**
 * Partition for distributed filesystem transport method using parquet files
 *
 * @param fileRanges List of files and ranges of row groups to read for those files
 * @param rangeCountMap Map representing how many file ranges exist for each file. Used for tracking and cleanup.
 */
final case class VerticaDistributedFilesystemPartitionTemplate[M](fileRanges: ParquetFileRangeIterator, rangeCountMap: M) extends VerticaPartition

object VerticaDistributedFilesystemPartitionTemplate {
  type VerticaDistributedFilesystemPartitionIterator = VerticaDistributedFilesystemPartitionTemplate[Unit]
  type VerticaDistributedFilesystemPartition = VerticaDistributedFilesystemPartitionTemplate[Map[String, Int]]

  def makeVerticaDistribuedFSPartitionIterator(iterator: ParquetFileRangeIterator): VerticaDistributedFilesystemPartitionIterator = {
    VerticaDistributedFilesystemPartitionTemplate(iterator, ())
  }

  def makeVerticaDistribuedFSPartition(
                                        iterator: VerticaDistributedFilesystemPartitionIterator,
                                        rangeCountMap: Map[String, Int]): VerticaDistributedFilesystemPartition = {
    VerticaDistributedFilesystemPartitionTemplate(iterator.fileRanges, rangeCountMap)
  }
}


/**
 * Implementation of the pipe to Vertica using a distributed filesystem as an intermediary layer.
 *
 * Dependencies such as the JDBCLayerInterface may be optionally passed in, this option is in place mostly for tests. If not passed in, they will be instantiated here.
 */
class VerticaDistributedFilesystemReadPipe(
                                            val config: DistributedFilesystemReadConfig,
                                            val fileStoreLayer: FileStoreLayerInterface,
                                            val jdbcLayer: JdbcLayerInterface,
                                            val schemaTools: SchemaToolsInterface,
                                            val cleanupUtils: CleanupUtilsInterface = CleanupUtils,
                                            val dataSize: Int = 1
                                          ) extends VerticaPipeInterface with VerticaPipeReadInterface {
  private val logger: Logger = config.getLogger(classOf[VerticaDistributedFilesystemReadPipe])

  // File size params. The max size of a single file, and the max size of an individual row group inside the parquet file.
  // TODO: Tune these with performance tests. Determine whether a single value is suitable or if we need to add a user option.
  private val maxFileSize = 512
  private val maxRowGroupSize = 64


  private def retrieveMetadata(): ConnectorResult[VerticaMetadata] = {
    schemaTools.readSchema(this.jdbcLayer, this.config.tablename.getFullTableName) match {
      case Right(schema) => Right(VerticaReadMetadata(schema))
      case Left(err) => Left(SchemaDiscoveryError(Some(err)))
    }
  }

  private def addPushdownFilters(pushdownFilters: List[PushdownFilter]): String = {
    pushdownFilters match {
      case Nil => ""
      case _ => " WHERE " + pushdownFilters.map(_.getFilterString).mkString(" AND ")
    }
  }

  /**
   * Gets metadata, either cached in configuration object or retrieved from Vertica if we haven't yet.
   */
  override def getMetadata: ConnectorResult[VerticaMetadata] = {
    this.config.metadata match {
      case Some(data) => Right(data)
      case None => this.retrieveMetadata()
    }
  }

  /**
   * Returns the default number of rows to read/write from this pipe at a time.
   */
  override def getDataBlockSize: ConnectorResult[Long] = Right(dataSize)

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

  private def getPartitionInfo(fileMetadata: Seq[ParquetFileMetadata], rowGroupRoom: Int): PartitionInfo = {
    // Now, create partitions splitting up files roughly evenly
    var i = 0
    var partitions = List[VerticaDistributedFilesystemPartitionIterator]()
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
          val partition = makeVerticaDistribuedFSPartitionIterator(ParquetFileRangeIterator(curFileRanges.toIterator))
          partitions = partitions :+ partition
          curFileRanges = List[ParquetFileRange]()
          i = 0
          low = j + 1
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
      val partition = makeVerticaDistribuedFSPartitionIterator(ParquetFileRangeIterator(curFileRanges.toIterator))
      partitions = partitions :+ partition
    }

    // Add range count map info to partition
    val finalPartitions = partitions.map(part => makeVerticaDistribuedFSPartition(part, rangeCountMap.toMap))

    PartitionInfo(finalPartitions.toArray)
  }

  private def getColumnNames(requiredSchema: StructType): ConnectorResult[String] = {
    schemaTools.getColumnInfo(jdbcLayer, config.tablename.getFullTableName) match {
      case Left(err) => Left(CastingSchemaReadError(err))
      case Right(columnDefs) => Right(schemaTools.makeColumnsString(columnDefs, requiredSchema))
    }
  }

  /**
   * Initial setup for the whole read operation. Called by driver.
   */
  override def doPreReadSteps(): ConnectorResult[PartitionInfo] = {
    val fileStoreConfig = config.fileStoreConfig
    val delimiter = if(fileStoreConfig.address.takeRight(1) == "/" || fileStoreConfig.address.takeRight(1) == "\\") "" else "/"
    val hdfsPath = fileStoreConfig.address + delimiter + config.tablename.name
    logger.debug("Export path: " + hdfsPath)

    val ret: ConnectorResult[PartitionInfo] = for {
      _ <- getMetadata

      // Create unique directory for session
      _ = logger.debug("Creating unique directory: " + fileStoreConfig.address)

      _ <- fileStoreLayer.createDir(fileStoreConfig.address) match {
        case Left(err) =>
          err.getError match {
            case CreateDirectoryAlreadyExistsError() =>
              logger.debug("Directory already existed: " + fileStoreConfig.address)
              Right(())
            case _ => Left(err.context("Failed to create directory: " + fileStoreConfig.address))
          }
        case Right(_) => Right(())
      }


      // Safety check: Remove export directory if it exists (Vertica must create this dir)
      _ <- fileStoreLayer.removeDir(hdfsPath)

      // File permissions.
      // TODO: Add file permission option w/ default value '700'
      filePermissions = "777"

      cols <- getColumnNames(this.config.getRequiredSchema)

      exportStatement = "EXPORT TO PARQUET(" +
        "directory = '" + hdfsPath +
        "', fileSizeMB = " + maxFileSize +
        ", rowGroupSizeMB = " + maxRowGroupSize +
        ", fileMode = '" + filePermissions +
        "', dirMode = '" + filePermissions +
        "') AS " + "SELECT " + cols + " FROM " +
        config.tablename.getFullTableName + this.addPushdownFilters(this.config.getPushdownFilters) + ";"

      _ <- jdbcLayer.execute(exportStatement).leftMap(err => ExportFromVerticaError(err))

      // Retrieve all parquet files created by Vertica
      fileList <- fileStoreLayer.getFileList(hdfsPath)
      requestedPartitionCount <- if (fileList.isEmpty) {
        Left(PartitioningError().context("Returned file list was empty, so cannot create valid partition info"))
      } else {
        config.partitionCount match {
          case Some(count) => Right(count)
          case None => Right(fileList.size) // Default to 1 partition / file
        }
      }

      fileMetadata <- fileList.toList.traverse(filename => fileStoreLayer.getParquetFileMetadata(filename))
      totalRowGroups = fileMetadata.map(_.rowGroupCount).sum

      partitionCount = if (totalRowGroups < requestedPartitionCount) {
        logger.info("Less than " + requestedPartitionCount + " partitions required, only using " + totalRowGroups)
        totalRowGroups
      } else {
        requestedPartitionCount
      }

      // Get room per partition for row groups in order to split the groups up evenly
      extraSpace = if(totalRowGroups % partitionCount == 0) 0 else 1
      rowGroupRoom = (totalRowGroups / partitionCount) + extraSpace

      partitionInfo = getPartitionInfo(fileMetadata, rowGroupRoom)
    } yield partitionInfo

    // If there's an error, cleanup
    ret match {
      case Left(_) => cleanupUtils.cleanupAll(fileStoreLayer, hdfsPath)
      case _ => ()
    }
    jdbcLayer.close()
    ret
  }

  var partition : Option[VerticaDistributedFilesystemPartition] = None
  var currentFileRange: Option[ParquetFileRange] = None

  /**
   * Initial setup for the read of an individual partition. Called by executor.
   */
  def startPartitionRead(verticaPartition: VerticaPartition): ConnectorResult[Unit] = {
    for {
      part <- verticaPartition match {
        case p: VerticaDistributedFilesystemPartition => Right(p)
        case _ => Left(InvalidPartition())
      }
      _ = this.partition = Some(part)

      _ = this.currentFileRange = part.fileRanges.next()

      // Check if empty and initialize with first file range
      _ <- this.currentFileRange match {
        case None => Left(PartitionNoFilesReadError())
        case Some(fileRange) => fileStoreLayer.openReadParquetFile(fileRange)
      }
    } yield ()
  }

  private def getCleanupInfo(part: VerticaDistributedFilesystemPartition, curRange: ParquetFileRange): Option[FileCleanupInfo] = {
    part.rangeCountMap match {
      case rangeCountMap if rangeCountMap.contains (curRange.filename) => curRange.rangeIdx match {
        case Some(rangeIdx) => Some(FileCleanupInfo (curRange.filename, rangeIdx, rangeCountMap(curRange.filename)))
        case None =>
          logger.warn ("Missing range count index. Not performing any cleanup for file " + curRange.filename)
          None
      }
      case _ =>
        logger.warn ("Missing value in range count map. Not performing any cleanup for file " + curRange.filename)
        None
    }
  }

  private def cleanupOldFilesIfRequired(partition: VerticaDistributedFilesystemPartition, curRange: ParquetFileRange): Unit = {
    this.getCleanupInfo(partition, curRange) match {
      case Some(cleanupInfo) => cleanupUtils.checkAndCleanup(fileStoreLayer, cleanupInfo) match {
        case Left(err) => logger.warn("Ran into error when calling cleaning up. Treating as non-fatal. Err: " + err.getFullContext)
        case Right(_) => ()
      }
      case None => ()
    }
  }

  private def startReadingNextParquetFile(partition: VerticaDistributedFilesystemPartition, parquetFileRange: ParquetFileRange) : ConnectorResult[Option[DataBlock]] = {
    this.currentFileRange = partition.fileRanges.next()
    this.currentFileRange match {
      case Some(fileRange) => for {
          _ <- Right(this.cleanupOldFilesIfRequired(partition, parquetFileRange))
          _ <- fileStoreLayer.closeReadParquetFile()
          _ <- fileStoreLayer.openReadParquetFile(fileRange)
          data <- fileStoreLayer.readDataFromParquetFile(dataSize)
        } yield data
      case None => Right(None)
    }
  }

  private def readDataFromParquet(
                                   partition: VerticaDistributedFilesystemPartition,
                                   parquetFileRange: ParquetFileRange): ConnectorResult[Option[DataBlock]] = {
    val ret: ConnectorResult[Option[DataBlock]] = for {
      optData <- fileStoreLayer.readDataFromParquetFile(dataSize)
      data <- optData match {
        case None => this.startReadingNextParquetFile(partition, parquetFileRange)
        case Some(data) => Right(Some(data))
      }
    } yield data

    // If there was an underlying error, call cleanup
    (ret, getCleanupInfo(partition, parquetFileRange)) match {
      case (Left(_), Some(cleanupInfo)) => cleanupUtils.checkAndCleanup(fileStoreLayer, cleanupInfo)
      case _ => ()
    }
    ret
  }

  private def readPartition(partition: VerticaDistributedFilesystemPartition): ConnectorResult[Option[DataBlock]] = {
    this.currentFileRange match {
      case Some(parquetFileRange) => this.readDataFromParquet(partition, parquetFileRange)
      case None => Right(None)
    }
  }

  /**
   * Reads a block of data to the underlying source. Called by executor.
   */
  def readData: ConnectorResult[Option[DataBlock]] = {
    this.partition match {
      case None => Left(UninitializedReadError())
      case Some(p) => this.readPartition(p)
    }
  }


  /**
   * Ends the read, doing any necessary cleanup. Called by executor once reading the partition is done.
   */
  def endPartitionRead(): ConnectorResult[Unit] = {
    jdbcLayer.close()
    fileStoreLayer.closeReadParquetFile()
  }

}

