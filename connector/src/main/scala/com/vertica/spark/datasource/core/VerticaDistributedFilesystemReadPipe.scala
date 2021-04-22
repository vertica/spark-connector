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
import com.vertica.spark.util.schema.SchemaToolsInterface
import com.vertica.spark.datasource.fs._
import com.vertica.spark.datasource.v2.PushdownFilter
import com.vertica.spark.util.cleanup.{CleanupUtilsInterface, FileCleanupInfo}
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
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
 * @param config Specific configuration data for read using intermediary filesystem
 * @param fileStoreLayer Dependency for communication with the intermediary filesystem
 * @param jdbcLayer Dependency for communication with the database over JDBC
 * @param schemaTools Dependency for schema conversion between Vertica and Spark
 * @param cleanupUtils Depedency for handling cleanup of files used in intermediary filesystem
 */
class VerticaDistributedFilesystemReadPipe(
                                            val config: DistributedFilesystemReadConfig,
                                            val fileStoreLayer: FileStoreLayerInterface,
                                            val jdbcLayer: JdbcLayerInterface,
                                            val schemaTools: SchemaToolsInterface,
                                            val cleanupUtils: CleanupUtilsInterface,
                                            val dataSize: Int = 1
                                          ) extends VerticaPipeInterface with VerticaPipeReadInterface {
  private val logger: Logger = LogProvider.getLogger(classOf[VerticaDistributedFilesystemReadPipe])

  // File size params. The max size of a single file, and the max size of an individual row group inside the parquet file.
  // TODO: Tune these with performance tests. Determine whether a single value is suitable or if we need to add a user option.
  private val maxFileSize = config.maxFileSize
  private val maxRowGroupSize = config.maxRowGroupSize


  private def retrieveMetadata(): ConnectorResult[VerticaMetadata] = {
    schemaTools.readSchema(this.jdbcLayer, this.config.tableSource) match {
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
   *
   * At current time, the only metadata is the table schema.
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
    var partitions = List[VerticaDistributedFilesystemPartition]()
    var curFileRanges = List[ParquetFileRange]()
    val rangeCountMap = scala.collection.mutable.Map[String, Int]()

    logger.info("Creating partitions.")
    for(m <- fileMetadata) {
      val size = m.rowGroupCount
      logger.debug("Splitting file " + m.filename + " with row group count " + size)
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
          logger.debug("Reached partition with file " + m.filename + " , range low: " +
            low + " , range high: " + j + " , idx: " + rangeIdx)
          i = 0
          low = j + 1
        }
        else if(j == size - 1){ // Reached end of file's row groups, add to file ranges
          val rangeIdx = incrementRangeMapGetIndex(rangeCountMap, m.filename)
          val frange = ParquetFileRange(m.filename, low, j, Some(rangeIdx))
          curFileRanges = curFileRanges :+ frange
          logger.debug("Reached end of file " + m.filename + " , range low: " +
            low + " , range high: " + j + " , idx: " + rangeIdx)
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

    PartitionInfo(partitions.toArray)
  }

  private def getColumnNames(requiredSchema: StructType): ConnectorResult[String] = {
    schemaTools.getColumnInfo(jdbcLayer, config.tableSource) match {
      case Left(err) => Left(err.context("Failed to get table schema when checking for fields that need casts."))
      case Right(columnDefs) => Right(schemaTools.makeColumnsString(columnDefs, requiredSchema))
    }
  }

  /**
   * Initial setup for the whole read operation. Called by driver.
   *
   * Will:
   * - Create a unique directory to export to
   * - Tell Vertica to export to a subdirectory there
   * - Parse the list of files Vertica exported, and create a list of PartitionInfo structs representing how to split up the read.
   *
   * Partitioning info is composed af a group of file ranges. A file range could represent reading a whole file or part of one.
   * This way a single file could be split up among 10 partitions to read, or a single partition could read 10 files.
   */
  override def doPreReadSteps(): ConnectorResult[PartitionInfo] = {
    val fileStoreConfig = config.fileStoreConfig
    val delimiter = if(fileStoreConfig.address.takeRight(1) == "/" || fileStoreConfig.address.takeRight(1) == "\\") "" else "/"
    val hdfsPath = fileStoreConfig.address + delimiter + config.tableSource.identifier
    logger.debug("Export path: " + hdfsPath)

    val ret: ConnectorResult[PartitionInfo] = for {
      _ <- getMetadata

      // Set Vertica to work with kerberos and HDFS
      _ <- config.jdbcConfig.auth match {
        case _: KerberosAuth =>
          logger.debug("Using kerberos auth.")
          jdbcLayer.configureKerberosToFilestore(fileStoreLayer)
        case _ =>
          logger.debug("Using regular auth.")
          Right(())
      }

      // Create unique directory for session
      perm = config.filePermissions
      _ = logger.debug("Creating unique directory: " + fileStoreConfig.address + " with permissions: " + perm)

      _ <- fileStoreLayer.createDir(fileStoreConfig.address, perm.toString) match {
        case Left(err) =>
          err.getError match {
            case CreateDirectoryAlreadyExistsError(_) =>
              logger.debug("Directory already existed: " + fileStoreConfig.address)
              Right(())
            case _ => Left(err.context("Failed to create directory: " + fileStoreConfig.address))
          }
        case Right(_) => Right(())
      }

      // Check if export is already done (previous call of this function)
      exportDone <- fileStoreLayer.fileExists(hdfsPath)

      // File permissions.
      filePermissions = config.filePermissions

      cols <- getColumnNames(this.config.getRequiredSchema)
      _ = logger.info("Columns requested: " + cols)

      pushdownSql = this.addPushdownFilters(this.config.getPushdownFilters)
      _ = logger.info("Pushdown filters: " + pushdownSql)

      exportSource = config.tableSource match {
        case tablename: TableName => tablename.getFullTableName
        case TableQuery(query, _) => "(" + query + ") AS x"
      }

      exportStatement = "EXPORT TO PARQUET(" +
        "directory = '" + hdfsPath +
        "', fileSizeMB = " + maxFileSize +
        ", rowGroupSizeMB = " + maxRowGroupSize +
        ", fileMode = '" + filePermissions +
        "', dirMode = '" + filePermissions +
        "') AS " + "SELECT " + cols + " FROM " +
        exportSource + pushdownSql + ";"

      // Export if not already exported
      _ <- if(exportDone) {
        logger.info("Export already done, skipping export step.")
        Right(())
      } else {
        logger.info("Exporting.")
        jdbcLayer.execute(exportStatement).leftMap(err => ExportFromVerticaError(err))
      }

      // Retrieve all parquet files created by Vertica
      fullFileList <- fileStoreLayer.getFileList(hdfsPath)
      parquetFileList = fullFileList.filter(x => x.endsWith(".parquet"))
      requestedPartitionCount <- if (parquetFileList.isEmpty) {
        Left(FileListEmptyPartitioningError())
      } else {
        config.partitionCount match {
          case Some(count) => Right(count)
          case None => Right(parquetFileList.size) // Default to 1 partition / file
        }
      }

      fileMetadata <- parquetFileList.toList.traverse(filename => fileStoreLayer.getParquetFileMetadata(filename))
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

      _ <- jdbcLayer.close()
    } yield partitionInfo

    // If there's an error, cleanup
    ret match {
      case Left(_) =>
        logger.info("Cleaning up all files in path: " + hdfsPath)
        cleanupUtils.cleanupAll(fileStoreLayer, hdfsPath)
        jdbcLayer.close()
      case _ => ()
    }

    ret
  }

  var partition : Option[VerticaDistributedFilesystemPartition] = None
  var fileIdx = 0

  /**
   * Initial setup for the read of an individual partition. Called by executor.
   */
  def startPartitionRead(verticaPartition: VerticaPartition): ConnectorResult[Unit] = {
    logger.info("Starting partition read.")
    for {
      part <- verticaPartition match {
          case p: VerticaDistributedFilesystemPartition => Right(p)
          case _ => Left(InvalidPartition())
        }
      _ = this.partition = Some(part)
      _ = this.fileIdx = 0

      // Check if empty and initialize with first file range
      ret <- part.fileRanges.headOption match {
        case None =>
          logger.warn("No files to read set on partition.")
          Left(DoneReading())
        case Some(head) =>
          fileStoreLayer.openReadParquetFile(head)
      }
    } yield ret
  }

  private def getCleanupInfo(part: VerticaDistributedFilesystemPartition, curIdx: Int): Option[FileCleanupInfo] = {
    logger.debug("Getting cleanup info for partition with idx " + curIdx)
    for {
      _ <- if (curIdx >= part.fileRanges.size) {
        logger.warn("Invalid fileIdx " + this.fileIdx + ", can't perform cleanup.")
        None
      } else {
        Some(())
      }

      curRange = part.fileRanges(curIdx)
      ret <- part.rangeCountMap match {
        case Some (rangeCountMap) if rangeCountMap.contains (curRange.filename) => curRange.rangeIdx match {
          case Some (rangeIdx) => Some (FileCleanupInfo (curRange.filename, rangeIdx, rangeCountMap (curRange.filename)))
          case None =>
            logger.warn ("Missing range count index. Not performing any cleanup for file " + curRange.filename)
            None
        }
        case None =>
          logger.warn ("Missing range count map. Not performing any cleanup for file " + curRange.filename)
          None
        case _ =>
          logger.warn ("Missing value in range count map. Not performing any cleanup for file " + curRange.filename)
          None
      }
    } yield ret
  }

  /**
   * Reads a block of data to the underlying source. Called by executor.
   *
   * May return empty block if at end of read. If attempting to read after end of read, DoneReading will be returned.
   */
  // scalastyle:off
  def readData: ConnectorResult[DataBlock] = {
    val part = this.partition match {
      case None => return Left(UninitializedReadError())
      case Some(p) => p
    }

    val ret = fileStoreLayer.readDataFromParquetFile(dataSize) match {
      case Left(err) => Left(err)
      case Right(data) =>
        if(data.data.nonEmpty) {
          Right(data)
        }
        else {
          logger.info("Hit done reading for file segment.")

          // Cleanup old file if required
          getCleanupInfo(part, this.fileIdx) match {
            case Some(cleanupInfo) => cleanupUtils.checkAndCleanup(fileStoreLayer, cleanupInfo) match {
              case Left(err) => logger.warn("Ran into error when calling cleaning up. Treating as non-fatal. Err: " + err.getFullContext)
              case Right(_) => ()
            }
            case None => logger.warn("No cleanup info found.")
          }

          // Next file
          this.fileIdx += 1
          if (this.fileIdx >= part.fileRanges.size) return Right(data)

          for {
            _ <- fileStoreLayer.closeReadParquetFile()
            _ <- fileStoreLayer.openReadParquetFile(part.fileRanges(this.fileIdx))
            data <- fileStoreLayer.readDataFromParquetFile(dataSize)
          } yield data
        }
    }

    // If there was an underlying error, call cleanup
    (ret, getCleanupInfo(part,this.fileIdx)) match {
      case (Left(_), Some(cleanupInfo)) => cleanupUtils.checkAndCleanup(fileStoreLayer, cleanupInfo) match {
        case Right(()) => ()
        case Left(err) => logger.warn("Ran into error when cleaning up: " + err.getFullContext)
      }
      case (Left(_), None) => logger.warn("No cleanup info found")
      case (Right(dataBlock), Some(cleanupInfo)) =>
        if(dataBlock.data.isEmpty) cleanupUtils.checkAndCleanup(fileStoreLayer, cleanupInfo) match {
          case Right(()) => ()
          case Left(err) => logger.warn("Ran into error when cleaning up: " + err.getFullContext)
        }
      case (Right(_), None) => ()
    }
    ret
  }


  /**
   * Ends the read, doing any necessary cleanup. Called by executor once reading the partition is done.
   */
  def endPartitionRead(): ConnectorResult[Unit] = {
    logger.info("Ending partition read.")
    fileStoreLayer.closeReadParquetFile()
  }

}

