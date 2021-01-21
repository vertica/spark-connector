package com.vertica.spark.datasource.fs

import java.util
import java.util.Collections

import com.vertica.spark.datasource.core.DataBlock
import com.vertica.spark.util.error.ConnectorError
import com.vertica.spark.util.error.ConnectorErrorType._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader, ParquetWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import cats.implicits._
import com.typesafe.scalalogging.Logger
import com.vertica.spark.config.{DistributedFilesystemReadConfig, DistributedFilesystemWriteConfig}
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO, RecordReader}

import scala.collection.JavaConversions._
import collection.JavaConverters._
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

case class HadoopFileStoreReader(reader: ParquetFileReader, columnIO: MessageColumnIO, recordConverter: RecordMaterializer[InternalRow]) {
  var recordReader: Option[RecordReader[InternalRow]] = None
  var curRow = 0L
  var rowCount = 0L

  def checkUpdateRecordReader = {
    if(curRow == rowCount){
      val pages = reader.readNextRowGroup()
      if(pages != null) {
        this.recordReader = Some(columnIO.getRecordReader(pages, recordConverter, FilterCompat.NOOP))
        this.rowCount = pages.getRowCount()
        this.curRow = 0
        println("Row count for page: " + rowCount)
      }
      else {
        // Done reading
        this.recordReader = None
        curRow = -1
      }
    }
  }

  def read(blockSize: Int) : Either[ConnectorError, DataBlock] = {
    (0 until blockSize).map(_ => Try {
      checkUpdateRecordReader
      recordReader match {
        case None => None
        case Some(reader) => Some(reader.read().copy())
      }
    } match {
      case Failure(exception) =>
        //logger.error("Error reading parquet file from HDFS.", exception)
        Left(ConnectorError(IntermediaryStoreReadError))
      case Success(v) => Right(v)
    }).toList.sequence match {
      case Left(err) => Left(err)
      case Right(list) => Right(DataBlock(list.flatten))
    }
  }

  def close() : Either[ConnectorError, DataBlock] = ???
}

class HadoopFileStoreLayer(
                 writeConfig: DistributedFilesystemWriteConfig,
                 readConfig: DistributedFilesystemReadConfig) extends FileStoreLayerInterface {
  val logger: Logger = readConfig.getLogger(classOf[HadoopFileStoreLayer])

  private var writer: Option[ParquetWriter[InternalRow]] = None
  private var reader: Option[HadoopFileStoreReader] = None

  private class VerticaParquetBuilder(file: Path) extends ParquetWriter.Builder[InternalRow, VerticaParquetBuilder](file: Path) {
    override protected def self: VerticaParquetBuilder = this

    protected def getWriteSupport(conf: Configuration) = new ParquetWriteSupport
  }

  def openWriteParquetFile(filename: String): Either[ConnectorError, Unit] = ???

  override def writeDataToParquetFile(filename: String, dataBlock: DataBlock): Either[ConnectorError, Unit] = ???

  override def closeWriteParquetFile(filename: String): Either[ConnectorError, Unit] = ???

  private def toSetMultiMap[K, V](map: util.Map[K, V] ) :  util.Map[K, util.Set[V]] = {
    val setMultiMap: util.Map[K, util.Set[V]] = new util.HashMap();
    for (entry <- map.entrySet()) {
      setMultiMap.put(entry.getKey(), Collections.singleton(entry.getValue()));
    }
    return Collections.unmodifiableMap(setMultiMap);
  }

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

    // Get reader
    val readerOrError = Try {
      val path = new Path(s"${filename}")
      //val inputfile = HadoopInputFile.fromPath(path, hdfsConfig)
      val options = (new ParquetReadOptions.Builder()).build()
      val fileReader = ParquetFileReader.open(hdfsConfig, path)

      val parquetFileMetadata = fileReader.getFooter().getFileMetaData()
      val fileSchema = parquetFileMetadata.getSchema();
      val fileMetadata = parquetFileMetadata.getKeyValueMetaData()
      val readContext = readSupport.init(new InitContext(hdfsConfig, toSetMultiMap(fileMetadata), fileSchema));

      // Create record converter
      val recordConverter = readSupport.prepareForRead(hdfsConfig, fileMetadata, fileSchema, readContext);

      // Set readers requested schema from read context
      val requestedSchema = readContext.getRequestedSchema()
      fileReader.setRequestedSchema(requestedSchema)

      // Column IO for record conversion
      val columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy())

      val strictTypeChecking = false
      val columnIO = columnIOFactory.getColumnIO(requestedSchema, fileSchema, strictTypeChecking);

      HadoopFileStoreReader(fileReader, columnIO, recordConverter)
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

  override def readDataFromParquetFile(filename: String, blockSize: Int): Either[ConnectorError, DataBlock] = {
    for{
      reader <- this.reader match {
        case Some (reader) => Right (reader)
        case None =>
          logger.error ("Error reading parquet file from HDFS: Reader was not initialized.")
          Left(ConnectorError(IntermediaryStoreReadError))
        }
      dataBlock <- reader.read(blockSize)
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

