import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalamock.scalatest.MockFactory
import java.sql.ResultSet
import java.sql.ResultSetMetaData

import com.vertica.spark.config._
import com.vertica.spark.util.schema._
import com.vertica.spark.datasource.core._
import ch.qos.logback.classic.Level
import com.vertica.spark.connector.fs.FileStoreLayerInterface
import org.apache.spark.sql.types._
import com.vertica.spark.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import com.vertica.spark.util.error.SchemaErrorType._
import com.vertica.spark.util.error.JdbcErrorType._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition

class VerticaDistributedFilesystemReadPipeTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest{

  val tablename = TableName("dummy", None)
  val jdbcConfig = JDBCConfig("1.1.1.1", 1234, "test", "test", "test", Level.ERROR)
  val fileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/test")

  override def afterAll() = {
  }

  it should "retrieve metadata when not provided" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, metadata = None)

    val mockSchemaTools = mock[SchemaToolsInterface]
    (mockSchemaTools.readSchema _).expects(*,tablename.name).returning(Right(new StructType()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[FileStoreLayerInterface], mock[JdbcLayerInterface], mockSchemaTools)

    pipe.getMetadata() match {
      case Left(err) => fail
      case Right(metadata) => assert(metadata.schema == new StructType())
    }
  }

  it should "use full schema" in {
    val fullTablename = TableName("table", Some("schema"))
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = fullTablename, metadata = None)

    val mockSchemaTools = mock[SchemaToolsInterface]
    (mockSchemaTools.readSchema _).expects(*,"schema.table").returning(Right(new StructType()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[FileStoreLayerInterface], mock[JdbcLayerInterface], mockSchemaTools)

    pipe.getMetadata()
  }

  it should "return cached metadata" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[FileStoreLayerInterface], mock[JdbcLayerInterface], mock[SchemaToolsInterface])

    pipe.getMetadata() match {
      case Left(err) => fail
      case Right(metadata) => assert(metadata.schema == new StructType())
    }
  }

  it should "return an error when there's an issue parsing schema" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = None)

    val mockSchemaTools = mock[SchemaToolsInterface]
      (mockSchemaTools.readSchema _).expects(*,tablename.name).returning(Left(List(SchemaError(MissingConversionError, "unknown"))))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[FileStoreLayerInterface], mock[JdbcLayerInterface], mockSchemaTools)

    pipe.getMetadata() match {
      case Left(err) => assert(err.err == SchemaDiscoveryError)
      case Right(metadata) => fail
    }
  }

  it should "call Vertica to export parquet files on pre read steps" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    val expectedAddress = fileStoreConfig.address + "/" + config.tablename.getFullTableName
    (fileStoreLayer.removeDir _).expects(expectedAddress).returning(Right())
    (fileStoreLayer.createDir _).expects(expectedAddress).returning(Right())
    (fileStoreLayer.getFileList _).expects(expectedAddress).returning(Right(Array[String]("example")))

    val jdbcLayer = mock[JdbcLayerInterface]
    val expectedJdbcCommand = "EXPORT TO PARQUET(directory = 'hdfs://example-hdfs:8020/tmp/test/dummy', fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT * FROM dummy;"
    (jdbcLayer.execute _).expects(expectedJdbcCommand).returning(Right())

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.doPreReadSteps() match {
      case Left(err) => assert(false)
      case Right(_) =>
    }
  }

  it should "return an error when there's a filesystem failure" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.removeDir _).expects(*).returning(Left(ConnectorError(FileSystemError)))

    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.doPreReadSteps() match {
      case Left(err) => assert(err.err == FileSystemError)
      case Right(_) => assert(false)
    }
  }

  it should "return an error when there's a JDBC failure" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.removeDir _).expects(*).returning(Right())
    (fileStoreLayer.createDir _).expects(*).returning(Right())

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*).returning(Left(JDBCLayerError(ConnectionError)))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.doPreReadSteps() match {
      case Left(err) => assert(err.err == ExportFromVerticaError)
      case Right(_) => assert(false)
    }
  }

  private def seqContains[T](seq: Seq[T], v: T) = {
    !(seq.filter(partition => partition.asInstanceOf[VerticaDistributedFilesystemPartition].filename == v)).isEmpty
  }

  it should "return partitioning info from pre-read steps based on files from filesystem" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    val expectedAddress = fileStoreConfig.address + "/" + config.tablename.getFullTableName
    (fileStoreLayer.removeDir _).expects(*).returning(Right())
    (fileStoreLayer.createDir _).expects(*).returning(Right())

    // Files returned by filesystem (mock of what vertica would create
    val exportedFiles = Array[String](expectedAddress+"/t1p1.parquet", expectedAddress+"/t1p2.parquet", expectedAddress+"/t1p3.parquet")
    (fileStoreLayer.getFileList _).expects(expectedAddress).returning(Right(exportedFiles))

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*).returning(Right())

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.doPreReadSteps() match {
      case Left(err) => assert(false)
      case Right(partitionInfo) =>
        val partitions = partitionInfo.partitionSeq
        assert(partitions.size == 3)
        assert(seqContains(partitions, exportedFiles(0)))
        assert(seqContains(partitions, exportedFiles(1)))
        assert(seqContains(partitions, exportedFiles(2)))
    }
  }

  it should "Return an error when there is a problem retrieving file list" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.removeDir _).expects(*).returning(Right())
    (fileStoreLayer.createDir _).expects(*).returning(Right())

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    (fileStoreLayer.getFileList _).expects(*).returning(Left(ConnectorError(FileSystemError)))

    pipe.doPreReadSteps() match {
      case Left(err) => assert(err.err == FileSystemError)
      case Right(_) => fail
    }
  }

  it should "Return an error when there are no files to create partitions from" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    val expectedAddress = fileStoreConfig.address + "/" + config.tablename.getFullTableName
    (fileStoreLayer.removeDir _).expects(*).returning(Right())
    (fileStoreLayer.createDir _).expects(*).returning(Right())

    // Files returned by filesystem (mock of what vertica would create
    val exportedFiles = Array[String]()
    (fileStoreLayer.getFileList _).expects(expectedAddress).returning(Right(exportedFiles))

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*).returning(Right())

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.doPreReadSteps() match {
      case Left(err) => assert(err.err == PartitioningError)
      case Right(_) => fail
    }
  }

  it should "Use filestore layer to read data" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val filename = "test.parquet"
    val v1: Int = 1
    val v2: Float = 2.0f
    val data = DataBlock(List(InternalRow(v1, v2) ))

    val partition = VerticaDistributedFilesystemPartition(filename)

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.openReadParquetFile _).expects(filename).returning(Right())
    (fileStoreLayer.readDataFromParquetFile _).expects(filename,*).returning(Right(data))
    (fileStoreLayer.closeReadParquetFile _).expects(filename).returning(Right())

    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])
    pipe.dataSize = 2

    pipe.startPartitionRead(partition) match {
      case Left(err) => fail
      case Right(_) => ()
    }

    pipe.readData match {
      case Left(err) => fail
      case Right(data) =>
        assert(data.data.size == 1)
        assert(data.data(0).getInt(0) == v1)
        assert(data.data(0).getFloat(1) == v2)
    }

    pipe.endPartitionRead() match {
      case Left(err) => fail
      case Right(_) => ()
    }
  }

  it should "Return an error if there is a partition type mismatch" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val filename = "test.parquet"
    val v1: Int = 1
    val v2: Float = 2
    val data = DataBlock(List(InternalRow(v1, v2) ))

    val partition = mock[VerticaPartition]

    val fileStoreLayer = mock[FileStoreLayerInterface]
    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.startPartitionRead(partition) match {
      case Left(err) => assert(err.err == InvalidPartition)
      case Right(_) => fail
    }
  }

  it should "Pass on errors from filestore layer on read start" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val filename = "test.parquet"
    val partition = VerticaDistributedFilesystemPartition(filename)

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.openReadParquetFile _).expects(filename).returning(Left(ConnectorError(StagingFsUrlMissingError)))

    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.startPartitionRead(partition) match {
      case Left(err) => assert(err.err == StagingFsUrlMissingError)
      case Right(_) => fail
    }
  }

  it should "Pass on errors from filestore layer on read" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val filename = "test.parquet"
    val partition = VerticaDistributedFilesystemPartition(filename)

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.openReadParquetFile _).expects(filename).returning(Right())
    (fileStoreLayer.readDataFromParquetFile _).expects(filename,*).returning(Left(ConnectorError(StagingFsUrlMissingError)))

    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.startPartitionRead(partition) match {
      case Left(err) => fail
      case Right(_) => ()
    }

    pipe.readData match {
      case Left(err) => assert(err.err == StagingFsUrlMissingError)
      case Right(_) => fail
    }
  }

  it should "Pass on errors from filestore layer on read end" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val filename = "test.parquet"
    val v1: Int = 1
    val v2: Float = 2.0f
    val data = DataBlock(List(InternalRow(v1, v2) ))

    val partition = VerticaDistributedFilesystemPartition(filename)

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.openReadParquetFile _).expects(filename).returning(Right())
    (fileStoreLayer.readDataFromParquetFile _).expects(filename,*).returning(Right(data))
    (fileStoreLayer.closeReadParquetFile _).expects(filename).returning(Left(ConnectorError(StagingFsUrlMissingError)))

    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.startPartitionRead(partition) match {
      case Left(err) => fail
      case Right(_) => ()
    }

    pipe.readData match {
      case Left(err) => fail
      case Right(_) => ()
    }

    pipe.endPartitionRead() match {
      case Left(err) => assert(err.err == StagingFsUrlMissingError)
      case Right(_) => fail
    }
  }

}
