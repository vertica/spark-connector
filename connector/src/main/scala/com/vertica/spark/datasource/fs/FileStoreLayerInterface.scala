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

package com.vertica.spark.datasource.fs

import java.util
import java.util.Collections

import com.vertica.spark.datasource.core.{DataBlock, ParquetFileRange}
import com.vertica.spark.util.error.{CloseReadError, CloseWriteError, ConnectorError, CreateDirectoryAlreadyExistsError, CreateDirectoryError, CreateFileAlreadyExistsError, CreateFileError, DoneReading, FileListError, FileStoreThrownError, IntermediaryStoreReadError, IntermediaryStoreReaderNotInitializedError, IntermediaryStoreWriteError, IntermediaryStoreWriterNotInitializedError, MissingHDFSImpersonationTokenError, OpenReadError, OpenWriteError, RemoveDirectoryError, RemoveFileError}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CommonConfigurationKeys, FileSystem, Path}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetOutputFormat, ParquetWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import cats.implicits._
import com.typesafe.scalalogging.Logger
import com.vertica.spark.config.{FileStoreConfig, LogProvider}
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.Text
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO, RecordReader}
import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.datasources.parquet.vertica.ParquetReadSupport
import org.apache.spark.sql.types.StructType

import collection.JavaConverters._
import scala.util.{Failure, Success, Try}

// Relevant parquet metadata
final case class ParquetFileMetadata(filename: String, rowGroupCount: Int)

/**
 * Interface for communicating with a filesystem.
 *
 * Contains common operations for a filesystem such as creating, removing, reading, and writing files
 */
trait FileStoreLayerInterface {

  // Write
  def openWriteParquetFile(filename: String) : ConnectorResult[Unit]
  def writeDataToParquetFile(data: DataBlock): ConnectorResult[Unit]
  def closeWriteParquetFile(): ConnectorResult[Unit]

  // Read
  def getParquetFileMetadata(filename: String) : ConnectorResult[ParquetFileMetadata]
  def openReadParquetFile(file: ParquetFileRange) : ConnectorResult[Unit]
  def readDataFromParquetFile(blockSize: Int): ConnectorResult[DataBlock]
  def closeReadParquetFile(): ConnectorResult[Unit]

  // Other FS
  def getFileList(filename: String): ConnectorResult[Seq[String]]
  def removeFile(filename: String) : ConnectorResult[Unit]
  def removeDir(filename: String) : ConnectorResult[Unit]
  def createFile(filename: String) : ConnectorResult[Unit]
  def createDir(filename: String, permission: String) : ConnectorResult[Unit]
  def fileExists(filename: String) : ConnectorResult[Boolean]

  def getImpersonationToken(user: String) : ConnectorResult[String]
}

final case class HadoopFileStoreReader(reader: ParquetFileReader, columnIO: MessageColumnIO, recordConverter: RecordMaterializer[InternalRow], fileRange: ParquetFileRange) {
  private var recordReader: Option[RecordReader[InternalRow]] = None
  private var curRow = 0L
  private var rowCount = 0L
  private var curRowGroup = 0L

  private def doneReading() : Unit = {
    this.recordReader = None
    rowCount = -1
  }

  def checkUpdateRecordReader(): Unit = {
    if(this.curRow == this.rowCount){
      while(this.curRowGroup < fileRange.minRowGroup) {
        reader.skipNextRowGroup()
        this.curRowGroup += 1
      }

      if(this.curRowGroup > fileRange.maxRowGroup) {
        this.doneReading()
      }
      else {
        val pages = reader.readNextRowGroup()
        if(pages != null) {
          this.recordReader = Some(columnIO.getRecordReader(pages, recordConverter, FilterCompat.NOOP))
          this.rowCount = pages.getRowCount
          this.curRow = 0
          this.curRowGroup += 1
        }
        else {
          this.doneReading()
        }
      }
    }

    this.curRow += 1

  }

  def read(blockSize: Int) : ConnectorResult[DataBlock] = {
    (0 until blockSize).map(_ => Try {
      this.checkUpdateRecordReader()
      recordReader match {
        case None => None
        case Some(reader) => Some(reader.read().copy())
      }
    } match {
      case Failure(exception) => Left(IntermediaryStoreReadError(exception)
        .context("Error reading parquet file from HDFS."))
      case Success(v) => Right(v)
    }).toList.sequence match {
      case Left(err) => Left(err)
      case Right(list) => Right(DataBlock(list.flatten))
    }
  }

  def close(): ConnectorResult[Unit] = {
    Try { this.reader.close() }
      .toEither
      .left.map(exception => CloseReadError(exception).context("Error closing read of parquet file from HDFS."))
  }
}

class HadoopFileStoreLayer(fileStoreConfig : FileStoreConfig, schema: Option[StructType]) extends FileStoreLayerInterface {
  val logger: Logger = LogProvider.getLogger(classOf[HadoopFileStoreLayer])

  private var writer: Option[ParquetWriter[InternalRow]] = None
  private var reader: Option[HadoopFileStoreReader] = None

  val hdfsConfig: Configuration = new Configuration()
  schema match {
    case Some(schema) =>
      logger.debug("Read and write support schema: " + schema)
      hdfsConfig.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, schema.json)
      hdfsConfig.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, schema.json)
      ParquetWriteSupport.setSchema(schema, hdfsConfig)
    case None => ()
  }
  hdfsConfig.set(SQLConf.PARQUET_BINARY_AS_STRING.key, "false")
  hdfsConfig.set(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, "true")
  hdfsConfig.set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, "false")
  hdfsConfig.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, "INT96")
  hdfsConfig.set(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key, "CORRECTED")
  hdfsConfig.set(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ.key, "CORRECTED")
  hdfsConfig.setEnum(ParquetOutputFormat.JOB_SUMMARY_LEVEL, JobSummaryLevel.NONE)
  hdfsConfig.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "000")

  private class VerticaParquetBuilder(file: Path) extends ParquetWriter.Builder[InternalRow, VerticaParquetBuilder](file: Path) {
    override protected def self: VerticaParquetBuilder = this

    protected def getWriteSupport(conf: Configuration) = new ParquetWriteSupport
  }

  def openWriteParquetFile(filename: String): ConnectorResult[Unit] = {
    logger.debug("Opening write to file: " + filename)
    val builder = new VerticaParquetBuilder(new Path(s"$filename"))

    val writerOrError = for {
      _ <- removeFile(filename)
      writer <- Try { builder.withConf(hdfsConfig)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()}.toEither.left.map(exception => OpenWriteError(exception).context("Error opening write to HDFS."))
    } yield writer

    writerOrError match {
      case Left(err) => Left(err)
      case Right(writer) =>
        this.writer = Some(writer)
        Right(())
    }
  }

  override def writeDataToParquetFile(dataBlock: DataBlock): ConnectorResult[Unit] = {
    for {
      writer <- this.writer match {
        case Some(reader) => Right(reader)
        case None => Left(IntermediaryStoreWriterNotInitializedError()
          .context("Error writing parquet file from HDFS"))
      }
      _ <- dataBlock.data.traverse(record => Try{writer.write(record)}
        .toEither.left.map(exception => IntermediaryStoreWriteError(exception)
          .context("Error writing parquet file to HDFS.")))
    } yield ()
  }

  override def closeWriteParquetFile(): ConnectorResult[Unit] = {
    for {
      writer <- this.writer match {
        case Some(reader) => Right(reader)
        case None => Left(IntermediaryStoreWriterNotInitializedError()
          .context("Error closing write of parquet file from HDFS"))
      }
      _ <- Try {writer.close()}.toEither.left.map(exception => CloseWriteError(exception)
        .context("Error closing write of parquet file to HDFS."))
    } yield ()
  }

  private def toSetMultiMap[K, V](map: util.Map[K, V] ): util.Map[K, util.Set[V]] = {
    val setMultiMap: util.Map[K, util.Set[V]] = new util.HashMap()
    for (entry <- map.entrySet().asScala) {
      setMultiMap.put(entry.getKey, Collections.singleton(entry.getValue))
    }
    Collections.unmodifiableMap(setMultiMap)
  }

  override def getParquetFileMetadata(filename: String) : ConnectorResult[ParquetFileMetadata] = {

    val path = new Path(s"$filename")

    Try {
      val inputFile = HadoopInputFile.fromPath(path, hdfsConfig)
      val reader = ParquetFileReader.open(inputFile)

      val rowGroupCount = reader.getRowGroups.size

      reader.close()
      ParquetFileMetadata(filename, rowGroupCount)
    }.toEither.left.map(exception => FileListError(exception)
      .context(s"Error getting metadata for file $filename."))
  }

  override def openReadParquetFile(file: ParquetFileRange): ConnectorResult[Unit] = {
    val filename = file.filename

    val readSupport = new ParquetReadSupport(
      convertTz = None,
      enableVectorizedReader = false,
      datetimeRebaseMode = LegacyBehaviorPolicy.CORRECTED
    )

    // Get reader
    val readerOrError = Try {
      val path = new Path(s"$filename")
      val inputFile = HadoopInputFile.fromPath(path, hdfsConfig)
      val fileReader = ParquetFileReader.open(inputFile)

      val parquetFileMetadata = fileReader.getFooter.getFileMetaData
      val fileSchema = parquetFileMetadata.getSchema
      val fileMetadata = parquetFileMetadata.getKeyValueMetaData
      val readContext = readSupport.init(new InitContext(hdfsConfig, toSetMultiMap(fileMetadata), fileSchema))

      // Create record converter
      val recordConverter = readSupport.prepareForRead(hdfsConfig, fileMetadata, fileSchema, readContext)

      // Set readers requested schema from read context
      val requestedSchema = readContext.getRequestedSchema
      fileReader.setRequestedSchema(requestedSchema)

      // Column IO for record conversion
      val columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy)

      val strictTypeChecking = false
      val columnIO = columnIOFactory.getColumnIO(requestedSchema, fileSchema, strictTypeChecking)

      HadoopFileStoreReader(fileReader, columnIO, recordConverter, file)
    }.toEither.left.map(exception => OpenReadError(exception).context("Error creating Parquet Reader"))

    readerOrError match {
      case Right(reader) =>
        this.reader = Some(reader)
        Right(())
      case Left(err) => Left(err)
    }
  }

  override def readDataFromParquetFile(blockSize: Int): ConnectorResult[DataBlock] = {
    for {
      dataBlock <- for {
          reader <- this.reader match {
            case Some(reader) => Right(reader)
            case None => Left(IntermediaryStoreReaderNotInitializedError()
              .context("Error reading parquet file from HDFS"))
          }
          dataBlock <- reader.read(blockSize)
        } yield dataBlock

    } yield dataBlock
  }

  override def closeReadParquetFile(): ConnectorResult[Unit] = {
    val r = for {
      reader <- this.reader match {
        case Some(reader) => Right(reader)
        case None => Left(IntermediaryStoreReaderNotInitializedError()
          .context("Error closing read of parquet file from HDFS"))
      }
      _ <- reader.close()
    } yield ()
    this.reader = None
    r
  }

  override def getFileList(filename: String): ConnectorResult[Seq[String]] = {
    this.useFileSystem(filename, (fs, path) =>
      Try {fs.listStatus(path)} match {
        case Success(fileStatuses) => Right(fileStatuses.map(_.getPath.toString).toSeq)
        case Failure(exception) => Left(FileListError(exception).context("Error getting file list from HDFS."))
      })
  }

  override def removeFile(filename: String): ConnectorResult[Unit] = {
    this.useFileSystem(filename, (fs, path) =>
      if (fs.exists(path)) {
        Try{fs.delete(path, true); ()}.toEither.left.map(exception => RemoveFileError(path, exception)
          .context("Error removing HDFS file."))
      } else {
        Right(())
      })
  }

  override def removeDir(filename: String): ConnectorResult[Unit] = {
    this.useFileSystem(filename, (fs, path) =>
      if (fs.exists(path)) {
        Try{fs.delete(path, true); ()}.toEither.left.map(exception => RemoveDirectoryError(path, exception)
          .context("Error removing HDFS directory."))
      } else {
        Right(())
      })
  }

  override def createFile(filename: String): ConnectorResult[Unit] = {
    this.useFileSystem(filename, (fs, path) =>
      if (!fs.exists(path)) {
        Try {fs.create(path); ()}.toEither.left.map(exception => CreateFileError(path, exception)
          .context("Error creating HDFS file."))
      } else {
        Left(CreateFileAlreadyExistsError(filename))
      })
  }

  override def createDir(filename: String, permission: String): ConnectorResult[Unit] = {
    val perms = new FsPermission(permission)
    this.useFileSystem(filename, (fs, path) =>
      if (!fs.exists(path)) {
        logger.debug("Making path " + path + " with permissions: " + perms.toString)
        Try {fs.mkdirs(path, perms); ()}.toEither.left.map(exception => CreateDirectoryError(path, exception)
          .context("Error creating HDFS directory."))
      } else {
        Left(CreateDirectoryAlreadyExistsError(filename))
      })
  }

  def fileExists(filename: String) : ConnectorResult[Boolean] = {
    this.useFileSystem(filename, (fs, path) =>
      Right(fs.exists(path)))
  }

  // scalastyle:off
  override def getImpersonationToken(user: String) : ConnectorResult[String] = {
    this.useFileSystem(fileStoreConfig.address, (fs, _) => {
      var hdfsToken = ""

      val tokens = fs.addDelegationTokens(user, null)
      val itr = tokens.iterator
      while (itr.hasNext) {
        val token = itr.next();
        logger.debug("Hadoop impersonation: IT kind: " + token.getKind.toString)
        if (token.getKind.equals(new Text("HDFS_DELEGATION_TOKEN"))) {
          hdfsToken = token.encodeToUrlString
        }
      }

      if(hdfsToken.nonEmpty) {
        Right(hdfsToken)
      } else {
        Left(MissingHDFSImpersonationTokenError(user, fileStoreConfig.address))
      }
    })
  }

  private def useFileSystem[T](filename: String,
                               fsAction: (FileSystem, Path) => ConnectorResult[T]): ConnectorResult[T] = {
    // Path for directory of files
    logger.debug("Filestore path: " + filename)

    // Get list of partitions
    Try {
      val path = new Path(s"$filename")
      val fs = path.getFileSystem(hdfsConfig)
      fsAction(fs, path)
    } match {
      case Success(value) => value
      case Failure(exception) => Left(FileStoreThrownError(exception))
    }
  }
}

