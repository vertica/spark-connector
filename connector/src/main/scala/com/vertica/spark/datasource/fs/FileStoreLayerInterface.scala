package com.vertica.spark.datasource.fs

import java.util
import java.util.Collections

import com.vertica.spark.datasource.core.{DataBlock, ParquetFileRange}
import com.vertica.spark.util.error.ConnectorError
import com.vertica.spark.util.error.ConnectorErrorType._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import cats.implicits._
import com.typesafe.scalalogging.Logger
import com.vertica.spark.config.{DistributedFilesystemReadConfig, DistributedFilesystemWriteConfig, FileStoreConfig, LogProvider}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO, RecordReader}
import org.apache.spark.sql.types.StructType

import collection.JavaConverters._
import scala.util.{Failure, Success, Try}

// Relevant parquet metadata
final case class ParquetFileMetadata(filename: String, rowGroupCount: Int)

/**
 * Interface for communicating with a filesystem.
 *
 * Contains common operations for a filesystem such as creating, removing, reading, and writing files
 * TODO: Leaving out individual function javadoc comments for now as we plan to redesign this interface
 */
trait FileStoreLayerInterface {

  // Write
  def openWriteParquetFile(filename: String) : Either[ConnectorError, Unit]
  def writeDataToParquetFile(data: DataBlock): Either[ConnectorError, Unit]
  def closeWriteParquetFile(): Either[ConnectorError, Unit]

  // Read
  def getParquetFileMetadata(filename: String) : Either[ConnectorError, ParquetFileMetadata]
  def openReadParquetFile(file: ParquetFileRange) : Either[ConnectorError, Unit]
  def readDataFromParquetFile(blockSize: Int): Either[ConnectorError, DataBlock]
  def closeReadParquetFile(): Either[ConnectorError, Unit]

  // Other FS
  def getFileList(filename: String): Either[ConnectorError, Seq[String]]
  def removeFile(filename: String) : Either[ConnectorError, Unit]
  def removeDir(filename: String) : Either[ConnectorError, Unit]
  def createFile(filename: String) : Either[ConnectorError, Unit]
  def createDir(filename: String) : Either[ConnectorError, Unit]
  def fileExists(filename: String) : Either[ConnectorError, Boolean]
}

final case class HadoopFileStoreReader(reader: ParquetFileReader, columnIO: MessageColumnIO, recordConverter: RecordMaterializer[InternalRow], fileRange: ParquetFileRange, logProvider: LogProvider) {
  private val logger = logProvider.getLogger(classOf[HadoopFileStoreReader])

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

  def read(blockSize: Int) : Either[ConnectorError, DataBlock] = {
    (0 until blockSize).map(_ => Try {
      this.checkUpdateRecordReader()
      recordReader match {
        case None => None
        case Some(reader) => Some(reader.read().copy())
      }
    } match {
      case Failure(exception) =>
        logger.error("Error reading parquet file from HDFS.", exception)
        Left(ConnectorError(IntermediaryStoreReadError))
      case Success(v) =>
        Right(v)
    }).toList.sequence match {
      case Left(err) => Left(err)
      case Right(list) => Right(DataBlock(list.flatten))
    }
  }

  def close() : Either[ConnectorError, Unit] = {
    Try{
      this.reader.close()
    } match {
      case Success (_) => Right (())
      case Failure (exception) =>
        logger.error ("Error closing read of parquet file from HDFS.", exception)
        Left (ConnectorError (CloseReadError))
    }
  }
}

class HadoopFileStoreLayer(logProvider: LogProvider, schema: Option[StructType]) extends FileStoreLayerInterface {
  val logger: Logger = logProvider.getLogger(classOf[HadoopFileStoreLayer])

  private var writer: Option[ParquetWriter[InternalRow]] = None
  private var reader: Option[HadoopFileStoreReader] = None

  // This variable is necessary for now until we change the interface's method of signalling when the read is done
  private var done = false

  val hdfsConfig: Configuration = new Configuration()
  schema match {
    case Some(schema) =>
      hdfsConfig.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, schema.json)
      hdfsConfig.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, schema.json)
    case None => ()
  }
  hdfsConfig.set(SQLConf.PARQUET_BINARY_AS_STRING.key, "false")
  hdfsConfig.set(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, "true")
  hdfsConfig.set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, "false")
  hdfsConfig.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, "INT96")


  private class VerticaParquetBuilder(file: Path) extends ParquetWriter.Builder[InternalRow, VerticaParquetBuilder](file: Path) {
    override protected def self: VerticaParquetBuilder = this

    protected def getWriteSupport(conf: Configuration) = new ParquetWriteSupport
  }

  def openWriteParquetFile(filename: String): Either[ConnectorError, Unit] = {
    val builder = new VerticaParquetBuilder(new Path(s"$filename"))

    val writerOrError = for {
      _ <- removeFile(filename) match {
        case Right(_) => Right(())
        case Left(ConnectorError(RemoveFileDoesNotExistError)) => Right(())
        case Left(err) => Left(err)
      }
      writer <- Try{builder.withConf(hdfsConfig).enableValidation().build()} match {
        case Success(writer) => Right(writer)
        case Failure(exception) =>
          logger.error("Error opening write to HDFS.", exception)
          Left(ConnectorError(OpenWriteError))
      }
    } yield writer

    writerOrError match {
      case Left(err) => Left(err)
      case Right(writer) =>
        this.writer = Some(writer)
        Right(())
    }
  }

  override def writeDataToParquetFile(dataBlock: DataBlock): Either[ConnectorError, Unit] = {
    for {
      writer <- this.writer match {
        case Some (reader) => Right (reader)
        case None =>
          logger.error ("Error writing parquet file from HDFS: Writer was not initialized.")
          Left(ConnectorError(IntermediaryStoreWriteError))
      }
      _ <- dataBlock.data.map(record => Try{writer.write(record)} match {
        case Failure(exception) =>
          logger.error("Error writing parquet file to HDFS.", exception)
          Left(ConnectorError(IntermediaryStoreWriteError))
        case Success(_) => Right(())
      }).sequence
    } yield ()
  }

  override def closeWriteParquetFile(): Either[ConnectorError, Unit] = {
    for {
      writer <- this.writer match {
        case Some(reader) => Right(reader)
        case None =>
          logger.error("Error writing parquet file from HDFS: Writer was not initialized.")
          Left(ConnectorError(IntermediaryStoreWriteError))
      }
      _ <- Try {writer.close()} match {
        case Success (_) => Right (())
        case Failure (exception) =>
          logger.error ("Error closing write of parquet file to HDFS.", exception)
          Left(ConnectorError (CloseWriteError))
      }
    } yield ()
  }

  private def toSetMultiMap[K, V](map: util.Map[K, V] ): util.Map[K, util.Set[V]] = {
    val setMultiMap: util.Map[K, util.Set[V]] = new util.HashMap()
    for (entry <- map.entrySet().asScala) {
      setMultiMap.put(entry.getKey, Collections.singleton(entry.getValue))
    }
    Collections.unmodifiableMap(setMultiMap)
  }

  override def getParquetFileMetadata(filename: String) : Either[ConnectorError, ParquetFileMetadata] = {

    val path = new Path(s"$filename")

    Try {
      val inputFile = HadoopInputFile.fromPath(path, hdfsConfig)
      val reader = ParquetFileReader.open(inputFile)

      val rowGroupCount = reader.getRowGroups.size

      reader.close()
      ParquetFileMetadata(filename, rowGroupCount)
    } match {
      case Success(metadata) => Right(metadata)
      case Failure(exception) =>
        logger.error(s"Error getting metadata for file $filename.", exception)
        Left(ConnectorError(FileListError))
    }
  }

  override def openReadParquetFile(file: ParquetFileRange): Either[ConnectorError, Unit] = {
    this.done = false
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

      HadoopFileStoreReader(fileReader, columnIO, recordConverter, file, logProvider)
    } match {
      case Success(r) => Right(r)
      case Failure(exception) =>
        logger.error("Error creating Parquet Reader", exception)
        Left(ConnectorError(OpenReadError))
    }

    readerOrError match {
      case Right(reader) =>
        this.reader = Some(reader)
        Right(())
      case Left(err) => Left(err)
    }
  }

  override def readDataFromParquetFile(blockSize: Int): Either[ConnectorError, DataBlock] = {
    if (this.done){
      println("DONE SET; DONE READING")
      return Left(ConnectorError(DoneReading))
    }

    val dataBlock = for{
      reader <- this.reader match {
        case Some (reader) => Right (reader)
        case None =>
          logger.error ("Error reading parquet file from HDFS: Reader was not initialized.")
          Left(ConnectorError(IntermediaryStoreReadError))
        }
      dataBlock <- reader.read(blockSize)
    } yield dataBlock

    dataBlock match {
      case Left(_) => ()
      case Right(block) => if(block.data.size < blockSize) {
        this.done = true
      }
    }

    dataBlock
  }

  override def closeReadParquetFile(): Either[ConnectorError, Unit] = {
    val r = for {
      reader <- this.reader match {
        case Some(reader) => Right(reader)
        case None =>
          logger.error("Error reading parquet file from HDFS: Reader was not initialized.")
          Left(ConnectorError(CloseReadError))
        }
      _ <- reader.close()
    } yield ()
    this.reader = None
    r
  }

  override def getFileList(filename: String): Either[ConnectorError, Seq[String]] = {
    this.useFileSystem(filename, (fs, path) =>
      Try {fs.listStatus(path)} match {
        case Success(fileStatuses) => Right(fileStatuses.map(_.getPath.toString).toSeq)
        case Failure(exception) =>
          logger.error("Error getting file list from HDFS.", exception)
          Left(ConnectorError(FileListError))
      })
  }

  override def removeFile(filename: String): Either[ConnectorError, Unit] = {
    this.useFileSystem(filename, (fs, path) =>
      if (fs.exists(path)) {
        Try{fs.delete(path, true)} match {
          case Success(_) => Right(())
          case Failure(exception) =>
            logger.error("Error removing HDFS file.", exception)
            Left(ConnectorError(RemoveFileError))
        }
      } else {
        Right(())
      })
  }

  override def removeDir(filename: String): Either[ConnectorError, Unit] = {
    this.useFileSystem(filename, (fs, path) =>
      if (fs.exists(path)) {
        Try{fs.delete(path, true)} match {
          case Success(_) => Right(())
          case Failure(exception) =>
            logger.error("Error removing HDFS directory.", exception)
            Left(ConnectorError(RemoveDirectoryError))
        }
      } else {
        Right(())
      })
  }

  override def createFile(filename: String): Either[ConnectorError, Unit] = {
    this.useFileSystem(filename, (fs, path) =>
      if (!fs.exists(path)) {
        Try {fs.create(path)} match {
          case Success(_) => Right(())
          case Failure(exception) =>
            logger.error("Error creating HDFS file.", exception)
            Left(ConnectorError(CreateFileError))
        }
      } else {
        Left(ConnectorError(CreateFileAlreadyExistsError))
      })
  }

  override def createDir(filename: String): Either[ConnectorError, Unit] = {
    this.useFileSystem(filename, (fs, path) =>
      if (!fs.exists(path)) {
        Try {fs.mkdirs(path)} match {
          case Success(_) => Right(())
          case Failure(exception) =>
            logger.error("Error creating HDFS directory.", exception)
            Left(ConnectorError(CreateDirectoryError))
        }
      } else {
        Left(ConnectorError(CreateDirectoryAlreadyExistsError))
      })
  }

  def fileExists(filename: String) : Either[ConnectorError, Boolean] = {
    this.useFileSystem(filename, (fs, path) =>
      Right(fs.exists(path)))
  }

  private def useFileSystem[T](filename: String,
                               fsAction: (FileSystem, Path) => Either[ConnectorError, T]): Either[ConnectorError, T] = {
    // Path for directory of files
    logger.debug("Filestore path: " + filename)

    // Get list of partitions
    val path = new Path(s"$filename")
    val fs = path.getFileSystem(hdfsConfig)
    val result = fsAction(fs, path)
    fs.close()
    result
  }
}

