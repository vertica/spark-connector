package com.vertica.spark.datasource.fs

import com.vertica.spark.datasource.core.DataBlock
import com.vertica.spark.util.error.ConnectorError
import com.vertica.spark.util.error.ConnectorErrorType._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import cats.implicits._
import com.typesafe.scalalogging.Logger
import com.vertica.spark.config.FileStoreConfig
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

trait FileStoreLayerInterface {
  // Other FS
  def getFileList(filename: String): Either[ConnectorError, Seq[String]]
  def removeFile(filename: String) : Either[ConnectorError, Unit]
  def removeDir(filename: String) : Either[ConnectorError, Unit]
  def createFile(filename: String) : Either[ConnectorError, Unit]
  def createDir(filename: String) : Either[ConnectorError, Unit]
}

trait FileStoreLayerWriteInterface {
  // Write
  def writeDataToParquetFile(data: DataBlock): Either[ConnectorError, Unit]

  def closeWriteParquetFile(): Either[ConnectorError, Unit]
}

trait FileStoreLayerReadInterface {
  // Read
  def readDataFromParquetFile(blockSize: Int): Either[ConnectorError, DataBlock]

  def closeReadParquetFile(): Either[ConnectorError, Unit]
}

class DummyFileStoreLayer extends FileStoreLayerInterface with FileStoreLayerReadInterface with FileStoreLayerWriteInterface {
  // Write
  def writeDataToParquetFile(data: DataBlock) : Either[ConnectorError, Unit] = ???
  def closeWriteParquetFile() : Either[ConnectorError, Unit] = ???

  // Read
  def readDataFromParquetFile(blockSize : Int) : Either[ConnectorError, DataBlock] = ???
  def closeReadParquetFile() : Either[ConnectorError, Unit] = ???

  // Other FS
  def getFileList(filename: String): Either[ConnectorError, Seq[String]] = ???
  def removeFile(filename: String) : Either[ConnectorError, Unit] = ???
  def removeDir(filename: String) : Either[ConnectorError, Unit] = ???
  def createFile(filename: String) : Either[ConnectorError, Unit] = ???
  def createDir(filename: String) : Either[ConnectorError, Unit] = ???
}

object HDFSWriter {

  private class VerticaParquetBuilder(file: Path) extends ParquetWriter.Builder[InternalRow, VerticaParquetBuilder](file: Path) {
    override protected def self: VerticaParquetBuilder = this

    protected def getWriteSupport(conf: Configuration) = new ParquetWriteSupport
  }

  // TODO: This only requires the config for the Logger. Update this when we change how the logger is passed in.
  def openWriteParquetFile(config: FileStoreConfig, schema: StructType): Either[ConnectorError, HDFSWriter] = {
    val logger: Logger = config.getLogger(classOf[HDFSWriter])
    val builder = new VerticaParquetBuilder(new Path(s"$config"))

    val hdfsConfig = new Configuration()
    hdfsConfig.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, schema.json)
    hdfsConfig.set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, "false")
    hdfsConfig.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, "INT96")

    for {
      _ <- HDFSLayer(config).removeFile(config.address) match {
        case Right(_) => Right(())
        case Left(ConnectorError(RemoveFileDoesNotExistError)) => Right(())
        case Left(err) => Left(err)
      }
      writer <- Try{builder.withConf(hdfsConfig).enableValidation().build()} match {
        case Success(writer) => Right(HDFSWriter(config, writer))
        case Failure(exception) =>
          logger.error("Error opening write to HDFS.", exception)
          Left(ConnectorError(OpenWriteError))
      }
    } yield writer
  }
}

// TODO: This only requires the config for the Logger. Update this when we change how the logger is passed in.
case class HDFSWriter(config: FileStoreConfig, writer: ParquetWriter[InternalRow]) extends FileStoreLayerWriteInterface {
  val logger: Logger = config.getLogger(classOf[HDFSWriter])
  override def writeDataToParquetFile(dataBlock: DataBlock): Either[ConnectorError, Unit] = {
    dataBlock.data.map(record => Try{this.writer.write(record)} match {
      case Failure(exception) =>
        logger.error("Error writing parquet file to HDFS.", exception)
        Left(ConnectorError(IntermediaryStoreWriteError))
      case Success(v) => Right(v)
    }).sequence_
  }

  override def closeWriteParquetFile(): Either[ConnectorError, Unit] = {
    Try{this.writer.close()} match {
      case Success(_) => Right(())
      case Failure(exception) =>
        logger.error("Error closing write of parquet file to HDFS.", exception)
        Left(ConnectorError(CloseWriteError))
    }
  }
}

object HDFSReader {

  // TODO: This only requires the config for the Logger. Update this when we change how the logger is passed in.
  def openReadParquetFile(config: FileStoreConfig, schema: StructType): Either[ConnectorError, HDFSReader] = {
    val logger: Logger = config.getLogger(classOf[HDFSReader])

    val readSupport = new ParquetReadSupport(
      convertTz = None,
      enableVectorizedReader = false,
      datetimeRebaseMode = LegacyBehaviorPolicy.CORRECTED
    )

    val hdfsConfig : Configuration = new Configuration()
    hdfsConfig.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, schema.json)
    hdfsConfig.set(SQLConf.PARQUET_BINARY_AS_STRING.key, "false")
    hdfsConfig.set(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, "true")

    for {
      _ <- HDFSLayer(config).createFile(config.address) match {
        case Right(_) => Right(())
        case Left(ConnectorError(CreateFileAlreadyExistsError)) => Right(())
        case Left(err) => Left(err)
      }
      reader <- Try{ParquetReader.builder(readSupport, new Path(config.address)).withConf(hdfsConfig).build()} match {
        case Success(reader) => Right(reader)
        case Failure(exception) =>
          logger.error("Error opening read to HDFS.", exception)
          Left(ConnectorError(OpenReadError))
      }
    } yield HDFSReader(config, reader)
  }
}

// TODO: This only requires the config for the Logger. Update this when we change how the logger is passed in.
case class HDFSReader(config: FileStoreConfig, private val reader: ParquetReader[InternalRow]) extends FileStoreLayerReadInterface {
  val logger: Logger = config.getLogger(classOf[HDFSReader])

  override def readDataFromParquetFile(blockSize: Int): Either[ConnectorError, DataBlock] = {
    (0 until blockSize).map(_ => Try{this.reader.read().copy()} match {
      case Failure(exception) =>
        logger.error("Error reading parquet file from HDFS.", exception)
        Left(ConnectorError(IntermediaryStoreReadError))
      case Success(v) => Right(v)
    }).toList.sequence.map(DataBlock)
  }

  override def closeReadParquetFile(): Either[ConnectorError, Unit] = {
    Try{this.reader.close()} match {
      case Success(_) => Right(())
      case Failure(exception) =>
        logger.error("Error closing read of parquet file from HDFS.", exception)
        Left(ConnectorError(CloseReadError))
    }
  }
}

// TODO: This only requires the config for the Logger. Update this when we change how the logger is passed in.
case class HDFSLayer(config: FileStoreConfig) extends FileStoreLayerInterface {
  val logger: Logger = config.getLogger(classOf[HDFSLayer])

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
        Left(ConnectorError(RemoveFileDoesNotExistError))
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
        Left(ConnectorError(RemoveDirectoryDoesNotExistError))
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

  private def useFileSystem[T](filename: String,
                               fsAction: (FileSystem, Path) => Either[ConnectorError, T]): Either[ConnectorError, T] = {
    val sparkSession = SparkSession.active

    // Path for directory of parquet files
    logger.debug("HDFS PATH: " + filename)

    // Get list of partitions
    val path = new Path(s"$filename")
    val fs = path.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val result = fsAction(fs, path)
    fs.close()
    result
  }
}

// TODO: This only requires the config for the Logger. Update this when we change how the logger is passed in.
case class HDFSWriteLayer(config: FileStoreConfig, private val writer: HDFSWriter) extends FileStoreLayerWriteInterface with FileStoreLayerInterface {
  val hdfsLayer: HDFSLayer = HDFSLayer(config)
  override def writeDataToParquetFile(data: DataBlock): Either[ConnectorError, Unit] = this.writer.writeDataToParquetFile(data)
  override def closeWriteParquetFile(): Either[ConnectorError, Unit] = this.writer.closeWriteParquetFile()
  override def getFileList(filename: String): Either[ConnectorError, Seq[String]] = hdfsLayer.getFileList(filename)
  override def removeFile(filename: String): Either[ConnectorError, Unit] = hdfsLayer.removeFile(filename)
  override def removeDir(filename: String): Either[ConnectorError, Unit] = hdfsLayer.removeDir(filename)
  override def createFile(filename: String): Either[ConnectorError, Unit] = hdfsLayer.createFile(filename)
  override def createDir(filename: String): Either[ConnectorError, Unit] = hdfsLayer.createDir(filename)
}

// TODO: This only requires the config for the Logger. Update this when we change how the logger is passed in.
case class HDFSReadLayer(config: FileStoreConfig, private val reader: HDFSReader) extends FileStoreLayerReadInterface with FileStoreLayerInterface {
  val hdfsLayer: HDFSLayer = HDFSLayer(config)
  override def readDataFromParquetFile(blockSize: Int): Either[ConnectorError, DataBlock] = this.reader.readDataFromParquetFile(blockSize)
  override def closeReadParquetFile(): Either[ConnectorError, Unit] = this.reader.closeReadParquetFile()
  override def getFileList(filename: String): Either[ConnectorError, Seq[String]] = hdfsLayer.getFileList(filename)
  override def removeFile(filename: String): Either[ConnectorError, Unit] = hdfsLayer.removeFile(filename)
  override def removeDir(filename: String): Either[ConnectorError, Unit] = hdfsLayer.removeDir(filename)
  override def createFile(filename: String): Either[ConnectorError, Unit] = hdfsLayer.createFile(filename)
  override def createDir(filename: String): Either[ConnectorError, Unit] = hdfsLayer.createDir(filename)
}