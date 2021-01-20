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
import com.vertica.spark.config.{DistributedFilesystemReadConfig, DistributedFilesystemWriteConfig}

import scala.util.{Failure, Success, Try}

trait FileStoreLayerInterface {
  // Write
  def openWriteParquetFile(filename: String) : Either[ConnectorError, Unit]
  def writeDataToParquetFile(filename: String, data: DataBlock): Either[ConnectorError, Unit]
  def closeWriteParquetFile(filename: String): Either[ConnectorError, Unit]

  // Read
  def openReadParquetFile(filename: String) : Either[ConnectorError, Unit]
  def readDataFromParquetFile(filename: String, blockSize: Int): Either[ConnectorError, DataBlock]
  def closeReadParquetFile(filename: String): Either[ConnectorError, Unit]

  // Other FS
  def getFileList(filename: String): Either[ConnectorError, Seq[String]]
  def removeFile(filename: String) : Either[ConnectorError, Unit]
  def removeDir(filename: String) : Either[ConnectorError, Unit]
  def createFile(filename: String) : Either[ConnectorError, Unit]
  def createDir(filename: String) : Either[ConnectorError, Unit]
}

class DummyFileStoreLayer extends FileStoreLayerInterface {
  // Write
  def openWriteParquetFile(filename: String) : Either[ConnectorError, Unit] = ???
  def writeDataToParquetFile(filename: String, data: DataBlock) : Either[ConnectorError, Unit] = ???
  def closeWriteParquetFile(filename: String) : Either[ConnectorError, Unit] = ???

  // Read
  def openReadParquetFile(filename: String) : Either[ConnectorError, Unit] = ???
  def readDataFromParquetFile(filename: String, blockSize : Int) : Either[ConnectorError, DataBlock] = ???
  def closeReadParquetFile(filename: String) : Either[ConnectorError, Unit] = ???

  // Other FS
  def getFileList(filename: String): Either[ConnectorError, Seq[String]] = ???
  def removeFile(filename: String) : Either[ConnectorError, Unit] = ???
  def removeDir(filename: String) : Either[ConnectorError, Unit] = ???
  def createFile(filename: String) : Either[ConnectorError, Unit] = ???
  def createDir(filename: String) : Either[ConnectorError, Unit] = ???
}

class HadoopFileStoreLayer(
                 writeConfig: DistributedFilesystemWriteConfig,
                 readConfig: DistributedFilesystemReadConfig) extends FileStoreLayerInterface {
  val logger: Logger = readConfig.getLogger(classOf[HadoopFileStoreLayer])

  private var writer: Option[ParquetWriter[InternalRow]] = None
  private var reader: Option[ParquetReader[InternalRow]] = None

  private class VerticaParquetBuilder(file: Path) extends ParquetWriter.Builder[InternalRow, VerticaParquetBuilder](file: Path) {
    override protected def self: VerticaParquetBuilder = this

    protected def getWriteSupport(conf: Configuration) = new ParquetWriteSupport
  }

  def openWriteParquetFile(filename: String): Either[ConnectorError, Unit] = ???

  override def writeDataToParquetFile(filename: String, dataBlock: DataBlock): Either[ConnectorError, Unit] = ???

  override def closeWriteParquetFile(filename: String): Either[ConnectorError, Unit] = ???

  override def openReadParquetFile(filename: String): Either[ConnectorError, Unit] = {

    val readSupport = new ParquetReadSupport(
      convertTz = None,
      enableVectorizedReader = false,
      datetimeRebaseMode = LegacyBehaviorPolicy.CORRECTED
    )

    val hdfsConfig: Configuration = new Configuration()
    hdfsConfig.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, readConfig.metadata.get.schema.json)
    hdfsConfig.set(SQLConf.PARQUET_BINARY_AS_STRING.key, "false")
    hdfsConfig.set(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, "true")

    val readerOrError = for {
      _ <- this.createFile(filename) match {
        case Right(_) => Right(())
        case Left(ConnectorError(CreateFileAlreadyExistsError)) => Right(())
        case Left(err) => Left(err)
      }
      reader <- Try {
        ParquetReader.builder(readSupport, new Path(filename)).withConf(hdfsConfig).build()
      } match {
        case Success(reader) => Right(reader)
        case Failure(exception) =>
          logger.error("Error opening read to HDFS.", exception)
          Left(ConnectorError(OpenReadError))
      }
    } yield reader

    readerOrError match {
      case Right(reader) =>
        this.reader = Some(reader)
        Right(())
      case Left(err) => Left(err)
    }
  }

  override def readDataFromParquetFile(filename: String, blockSize: Int): Either[ConnectorError, DataBlock] = {
    for {
      reader <- this.reader match {
        case Some (reader) => Right (reader)
        case None =>
          logger.error ("Error reading parquet file from HDFS: Reader was not initialized.")
          Left(ConnectorError(IntermediaryStoreReadError))
        }
      dataBlock <- (0 until blockSize).map(_ => Try {reader.read().copy()} match {
        case Failure(exception) =>
          logger.error("Error reading parquet file from HDFS.", exception)
          Left(ConnectorError(IntermediaryStoreReadError))
        case Success(v) => Right(v)
      }).toList.sequence.map(DataBlock)
    } yield dataBlock
  }

  override def closeReadParquetFile(filename: String): Either[ConnectorError, Unit] = {
    for {
      reader <- this.reader match {
        case Some(reader) => Right(reader)
        case None =>
          logger.error("Error reading parquet file from HDFS: Reader was not initialized.")
          Left(ConnectorError(CloseReadError))
        }
      _ <- Try {reader.close()} match {
        case Success (_) => Right (())
        case Failure (exception) =>
          logger.error ("Error closing read of parquet file from HDFS.", exception)
          Left (ConnectorError (CloseReadError))
        }
    } yield ()
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

    // Path for directory of files
    logger.debug("Filestore path: " + filename)

    // Get list of partitions
    val path = new Path(s"$filename")
    val fs = path.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val result = fsAction(fs, path)
    fs.close()
    result
  }
}

