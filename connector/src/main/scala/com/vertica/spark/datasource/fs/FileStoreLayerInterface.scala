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

import java.net.URI
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
import com.vertica.spark.config.{AWSAuth, AWSOptions, FileStoreConfig, LogProvider}
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.UserGroupInformation
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO, RecordReader}
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
  def getGlobStatus(pattern: String): ConnectorResult[Seq[String]]

  def getImpersonationToken(user: String) : ConnectorResult[String]
  def getAWSOptions: AWSOptions
}

final case class HadoopFileStoreReader(reader: ParquetFileReader, columnIO: MessageColumnIO, recordConverter: RecordMaterializer[InternalRow], fileRange: ParquetFileRange) {

  private var curRowGroup = fileRange.minRowGroup
  (0 until fileRange.minRowGroup).foreach(_ => reader.skipNextRowGroup)

  private var curRow = 0L
  private var rowCount = 0L
  private var currRecordReader: Option[RecordReader[InternalRow]] = nextRecordReader

  private def nextRecordReader: Option[RecordReader[InternalRow]] = {
    if (this.curRowGroup <= fileRange.maxRowGroup) {
      Option(reader.readNextRowGroup()) match {
        case Some(pages) =>
          this.rowCount = pages.getRowCount
          this.curRow = 0
          this.curRowGroup += 1
          Some(columnIO.getRecordReader(pages, recordConverter, FilterCompat.NOOP))
        case None => None
      }
    } else None
  }

  private def checkRecordReader(): Option[RecordReader[InternalRow]] = {
    this.currRecordReader = if (this.curRow == this.rowCount) {
      nextRecordReader
    } else this.currRecordReader
    this.curRow += 1
    this.currRecordReader
  }

  private def read: Either[ConnectorError, Option[InternalRow]] = {
    Try {
      this.checkRecordReader() match {
        case Some(reader) => Some(reader.read().copy())
        case None => None
      }
    } match {
      case Failure(exception) => Left(IntermediaryStoreReadError(exception)
        .context("Error reading parquet file from HDFS."))
      case Success(v) => Right(v)
    }
  }

  def read(blockSize: Int) : ConnectorResult[DataBlock] = {
    (0 until blockSize).map(_ => read).toList.sequence match {
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
  private val S3_ACCESS_KEY: String = "fs.s3a.access.key"
  private val S3_SECRET_KEY: String = "fs.s3a.secret.key"
  private val S3_SESSION_TOKEN: String = "fs.s3a.session.token"
  private val AWS_CREDENTIALS_PROVIDER: String = "fs.s3a.aws.credentials.provider"
  private val S3_ENDPOINT: String = "fs.s3a.endpoint"
  private val S3_ENABLE_SSL: String = "fs.s3a.connection.ssl.enabled"
  private val S3_ENABLE_PATH_STYLE: String = "fs.s3a.path.style.access"
  val logger: Logger = LogProvider.getLogger(classOf[HadoopFileStoreLayer])

  private val WEBHDFS_DELEGATION_TOKEN_TEXT = new Text("WEBHDFS delegation")
  private val SWEBHDFS_DELEGATION_TOKEN_TEXT = new Text("SWEBHDFS delegation")
  private val HDFS_DELEGATION_TOKEN_TEXT = new Text("HDFS_DELEGATION_TOKEN")

  private val LEGACY_PARQUET_REBASE_MODE_IN_WRITE = "spark.sql.legacy.parquet.datetimeRebaseModeInWrite"
  private val LEGACY_PARQUET_REBASE_MODE_IN_READ =  "spark.sql.legacy.parquet.datetimeRebaseModeInRead"
  private val LEGACY_PARQUET_INT96_REBASE_MODE_IN_WRITE = "spark.sql.legacy.parquet.int96RebaseModeInWrite"

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
  private val awsOptions = fileStoreConfig.awsOptions

  awsOptions.awsCredentialsProvider match {
    case Some(provider) =>
      hdfsConfig.set(AWS_CREDENTIALS_PROVIDER, provider.arg)
      logger.info(s"Setting $AWS_CREDENTIALS_PROVIDER: $provider")
    case None =>
      logger.info("Did not set AWS credentials provider for Hadoop config")
  }
  awsOptions.awsAuth match {
    case Some(auth) =>
      hdfsConfig.set(S3_ACCESS_KEY, auth.accessKeyId.arg)
      logger.info(s"Loaded $S3_ACCESS_KEY from ${auth.accessKeyId.origin}")
      hdfsConfig.set(S3_SECRET_KEY, auth.secretAccessKey.arg)
      logger.info(s"Loaded $S3_SECRET_KEY from ${auth.secretAccessKey.origin}")
    case None => logger.info("Did not set AWS auth for Hadoop config")
  }
  awsOptions.awsSessionToken match {
    case Some(token) =>
      hdfsConfig.set(S3_SESSION_TOKEN, token.arg)
      logger.info(s"Loaded $S3_SESSION_TOKEN from ${token.origin}")
    case None => logger.info("Did not set AWS session token for Hadoop config")
  }
  awsOptions.awsEndpoint match {
    case Some(endpoint) =>
      hdfsConfig.set(S3_ENDPOINT, endpoint.arg)
      logger.info(s"Loaded $S3_ENDPOINT from ${endpoint.origin}")
    case None => logger.debug("Did not set AWS endpoint, using default.")
  }
  awsOptions.enableSSL match {
    case Some(enable) =>
      hdfsConfig.set(S3_ENABLE_SSL, enable.arg)
      logger.info(s"Loaded $S3_ENABLE_SSL from ${enable.origin}")
    case None => logger.debug("Did not set AWS SSL enabled flag, using default of true.")
  }
  awsOptions.enablePathStyle match {
    case Some(enable) =>
      hdfsConfig.set(S3_ENABLE_PATH_STYLE, enable.arg)
      logger.info(s"Loaded $S3_ENABLE_PATH_STYLE from ${enable.origin}")
    case None => logger.debug("Did not set AWS path style enabled flag, using default of false.")
  }

  hdfsConfig.set(SQLConf.PARQUET_BINARY_AS_STRING.key, "false")
  hdfsConfig.set(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, "true")
  hdfsConfig.set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, "false")
  hdfsConfig.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, "INT96")
  // Don't use SQLConf because that breaks things for users on Spark 3.2
  hdfsConfig.set(LEGACY_PARQUET_REBASE_MODE_IN_WRITE, "CORRECTED")
  hdfsConfig.set(LEGACY_PARQUET_REBASE_MODE_IN_READ, "CORRECTED")
  hdfsConfig.set(LEGACY_PARQUET_INT96_REBASE_MODE_IN_WRITE, "CORRECTED")
  hdfsConfig.setEnum(ParquetOutputFormat.JOB_SUMMARY_LEVEL, JobSummaryLevel.NONE)
  hdfsConfig.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "000")

  private class VerticaParquetBuilder(file: Path) extends ParquetWriter.Builder[InternalRow, VerticaParquetBuilder](file: Path) {
    override protected def self: VerticaParquetBuilder = this

    protected def getWriteSupport(conf: Configuration) = new ParquetWriteSupport
  }

  def openWriteParquetFile(filename: String): ConnectorResult[Unit] = {
    logger.info("Opening write to file: " + filename)
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
    logger.info("Opening read from file: " + filename)
    val readSupport = new ParquetReadSupport(
      convertTz = None,
      enableVectorizedReader = false,
      datetimeRebaseMode = LegacyBehaviorPolicy.CORRECTED,
      int96RebaseMode = LegacyBehaviorPolicy.CORRECTED
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
        Try {
          val stream = fs.create(path);
          stream.write(0)
          stream.close()
          ()
        }.toEither.left.map(exception => CreateFileError(path, exception)
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

    // First, see if there is already a delegation token (this is the case when run under YARN)
    var hdfsToken: Option[String] = None
    val ugiUser = Option(UserGroupInformation.getLoginUser)
    ugiUser match {
      case None => ()
      case Some(user) =>
        logger.debug("Got UGI user.")
        val existingTokens = user.getCredentials.getAllTokens
        val itr = existingTokens.iterator
        while (itr.hasNext) {
          val token = itr.next()
          val tokenKind = token.getKind
          logger.debug("Existing token kind: " + tokenKind.toString)
          if (WEBHDFS_DELEGATION_TOKEN_TEXT.equals(tokenKind) ||
            SWEBHDFS_DELEGATION_TOKEN_TEXT.equals(tokenKind) ||
            HDFS_DELEGATION_TOKEN_TEXT.equals(tokenKind)) {
            hdfsToken = Some(token.encodeToUrlString)
          }
        }
    }

    this.useFileSystem(fileStoreConfig.address, (fs, _) => {
      val tokens = fs.addDelegationTokens(user, null)
      val itr = tokens.iterator
      while (itr.hasNext) {
        val token = itr.next();
        val tokenKind = token.getKind
        logger.debug("Hadoop impersonation: IT kind: " + tokenKind.toString)
        if (WEBHDFS_DELEGATION_TOKEN_TEXT.equals(tokenKind) ||
          SWEBHDFS_DELEGATION_TOKEN_TEXT.equals(tokenKind) ||
          HDFS_DELEGATION_TOKEN_TEXT.equals(tokenKind)) {
          hdfsToken = Some(token.encodeToUrlString)
        }
      }

      Right(())
    })

    hdfsToken match {
      case Some(token) => Right(token)
      case None => Left(MissingHDFSImpersonationTokenError(user, fileStoreConfig.address))
    }
  }

  override def getGlobStatus(pattern: String): ConnectorResult[Seq[String]] = {
    this.useFileSystem(pattern, (fs, path) =>
      Try {fs.globStatus(path)} match {
        case Success(fileStatuses) => Right(fileStatuses.map(_.getPath.toString).toSeq)
        case Failure(exception) => Left(FileListError(exception).context("Error getting file list from HDFS."))
      })
  }

  override def getAWSOptions: AWSOptions = {
    this.fileStoreConfig.awsOptions
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

