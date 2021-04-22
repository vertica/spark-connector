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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalamock.scalatest.MockFactory
import com.vertica.spark.config._
import com.vertica.spark.util.schema._
import com.vertica.spark.datasource.fs.{FileStoreLayerInterface, ParquetFileMetadata}
import org.apache.spark.sql.types._
import com.vertica.spark.datasource.jdbc.{JdbcLayerInterface, JdbcLayerParam}
import com.vertica.spark.datasource.v2.PushFilter
import com.vertica.spark.util.cleanup.{CleanupUtilsInterface, FileCleanupInfo}
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{EqualTo, GreaterThan, LessThan}

class VerticaDistributedFilesystemReadPipeTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest{

  private val tablename = TableName("dummy", None)
  private val jdbcConfig = JDBCConfig("1.1.1.1", 1234, "test", BasicJdbcAuth("test", "test"))
  private val fileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/test")
  private val metadata = new MetadataBuilder().putString("name", "col1").build()
  private val size = 32
  private val scale = 32
  private val expectedAddress = fileStoreConfig.address + "/" + tablename.name

  override def afterAll(): Unit = {
  }

  case class TestColumnDef(index: Int, colName: String, colType: Int, colTypeName: String, scale: Int, signed: Boolean, nullable: Boolean)

  private def makeReadConfig: DistributedFilesystemReadConfig = {
    DistributedFilesystemReadConfig(
      jdbcConfig = jdbcConfig,
      fileStoreConfig = fileStoreConfig,
      tableSource = tablename,
      partitionCount = None,
      metadata = Some(VerticaReadMetadata(new StructType())),
      ValidFilePermissions("777").getOrElse(throw new Exception("File perm error")), 64, 512)
  }

  private def makeIntColumnDef: ColumnDef = {
    ColumnDef("col1", java.sql.Types.INTEGER, "INT", size, scale, signed = false, nullable = true, metadata)
  }

  private def mockJdbcLayer(expectedJdbcCommand: String, expectedJdbcParams: Seq[JdbcLayerParam] = Seq()): JdbcLayerInterface = {
    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(expectedJdbcCommand, expectedJdbcParams).returning(Right())
    (jdbcLayer.close _).expects().returning(Right(()))
    jdbcLayer
  }

  private def mockFileStoreLayer(config: DistributedFilesystemReadConfig, expectedAdd: String = expectedAddress): FileStoreLayerInterface = {
    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createDir _).expects(*,*).returning(Right())
    (fileStoreLayer.fileExists _).expects(expectedAdd).returning(Right(false))
    (fileStoreLayer.getFileList _).expects(expectedAdd).returning(Right(Array[String]("example.parquet")))
    (fileStoreLayer.getParquetFileMetadata _).expects(*).returning(Right(ParquetFileMetadata("example", 4)))
    fileStoreLayer
  }

  private def mockSchemaTools(
                               columnDefs: List[ColumnDef],
                               columnsString: String,
                               requiredSchema: StructType = StructType(Nil),
                               tableSource: TableSource = tablename
                             ): SchemaToolsInterface = {
    val schemaTools = mock[SchemaToolsInterface]
    (schemaTools.getColumnInfo _).expects(*,tableSource).returning(Right(columnDefs))
    (schemaTools.makeColumnsString _).expects(columnDefs, requiredSchema).returning(columnsString)
    schemaTools
  }

  private def failOnError[T](result: ConnectorResult[T]): Unit = {
    result match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
  }

  it should "retrieve metadata when not provided" in {
    val config = makeReadConfig.copy(metadata = None)

    val mockSchemaTools = mock[SchemaToolsInterface]
    (mockSchemaTools.readSchema _).expects(*,tablename).returning(Right(new StructType()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[FileStoreLayerInterface], mock[JdbcLayerInterface], mockSchemaTools, mock[CleanupUtilsInterface])

    pipe.getMetadata match {
      case Left(_) => fail
      case Right(metadata) => assert(metadata.asInstanceOf[VerticaReadMetadata].schema == new StructType())
    }
  }

  it should "use full schema" in {
    val fullTablename = TableName("table", Some("schema"))
    val config = makeReadConfig.copy(tableSource = fullTablename, metadata = None)

    val mockSchemaTools = mock[SchemaToolsInterface]
    (mockSchemaTools.readSchema _).expects(*,fullTablename).returning(Right(new StructType()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[FileStoreLayerInterface], mock[JdbcLayerInterface], mockSchemaTools, mock[CleanupUtilsInterface])

    pipe.getMetadata
  }

  it should "return cached metadata" in {
    val config = makeReadConfig

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[FileStoreLayerInterface], mock[JdbcLayerInterface], mock[SchemaToolsInterface], mock[CleanupUtilsInterface])

    pipe.getMetadata match {
      case Left(_) => fail
      case Right(metadata) => assert(metadata.asInstanceOf[VerticaReadMetadata].schema == new StructType())
    }
  }

  it should "return an error when there's an issue parsing schema" in {
    val config = makeReadConfig.copy(metadata = None)

    val mockSchemaTools = mock[SchemaToolsInterface]
      (mockSchemaTools.readSchema _).expects(*,tablename).returning(Left(MissingSqlConversionError("unknown", "")))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[FileStoreLayerInterface], mock[JdbcLayerInterface], mockSchemaTools, mock[CleanupUtilsInterface])

    pipe.getMetadata match {
      case Left(err) => assert(err.getError match {
        case SchemaDiscoveryError(_) => true
        case _ => false
      })
      case Right(_) => fail
    }
  }

  it should "call Vertica to export parquet files on pre read steps" in {
    val config = makeReadConfig

    val fileStoreLayer = mockFileStoreLayer(config)

    val jdbcLayer = mock[JdbcLayerInterface]
    val expectedJdbcCommand = "EXPORT TO PARQUET(directory = 'hdfs://example-hdfs:8020/tmp/test/dummy', fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT col1 FROM \"dummy\";"
    (jdbcLayer.execute _).expects(expectedJdbcCommand, *).returning(Right())
    (jdbcLayer.close _).expects().returning(Right(()))

    val columnDef = ColumnDef("col1", java.sql.Types.REAL, "REAL", 32, 32, signed = false, nullable = true, metadata)

    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    this.failOnError(pipe.doPreReadSteps())
  }

  it should "call jdbc layer to set kerberos impersonation" in {
    val config = makeReadConfig.copy(jdbcConfig = jdbcConfig.copy(auth = KerberosAuth("user", "", "", "")))

    val fileStoreLayer = mockFileStoreLayer(config)

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.configureKerberosToFilestore _).expects(fileStoreLayer).returning(Right(()))
    (jdbcLayer.execute _).expects(*, *).returning(Right())
    (jdbcLayer.close _).expects().returning(Right(()))

    val columnDef = ColumnDef("col1", java.sql.Types.REAL, "REAL", 32, 32, signed = false, nullable = true, metadata)
    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    this.failOnError(pipe.doPreReadSteps())
  }

  it should "export using query" in {
    val query = TableQuery("SELECT * FROM t where n > 777", "")
    val config = makeReadConfig.copy(tableSource = query)

    val fileStoreLayer = mockFileStoreLayer(config, fileStoreConfig.address + "/" + query.identifier)

    val jdbcLayer = mock[JdbcLayerInterface]
    val expectedJdbcCommand = "EXPORT TO PARQUET(directory = 'hdfs://example-hdfs:8020/tmp/test/" + query.identifier +
      "', fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT col1 " +
      "FROM (SELECT * FROM t where n > 777) AS x;"
    (jdbcLayer.execute _).expects(expectedJdbcCommand, *).returning(Right())
    (jdbcLayer.close _).expects().returning(Right(()))

    val columnDef = ColumnDef("col1", java.sql.Types.REAL, "REAL", 32, 32, signed = false, nullable = true, metadata)

    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1", StructType(Nil), query)

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    this.failOnError(pipe.doPreReadSteps())
  }

  it should "return an error when there's a filesystem failure" in {
    val config = makeReadConfig

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createDir _).expects(*,*).returning(Right())
    (fileStoreLayer.fileExists _).expects(*).returning(Left(ParentDirMissingError("")))

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.close _).expects().returning(Right(()))

    val cleanupUtils = mock[CleanupUtilsInterface]
    (cleanupUtils.cleanupAll _).expects(*,*).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface], cleanupUtils)

    pipe.doPreReadSteps() match {
      case Left(err) => assert(err.getError == ParentDirMissingError(""))
      case Right(_) => fail
    }
  }

  it should "return an error when there's a JDBC failure" in {
    val config = makeReadConfig

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createDir _).expects(*,*).returning(Right())
    (fileStoreLayer.fileExists _).expects(*).returning(Right(false)).anyNumberOfTimes()

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*, *).returning(Left(ConnectionError(new Exception())))
    (jdbcLayer.close _).expects().returning(Right(()))

    val columnDef = ColumnDef("col1", java.sql.Types.REAL, "REAL", 32, 32, signed = false, nullable = true, metadata)

    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")

    val cleanupUtils = mock[CleanupUtilsInterface]
    (cleanupUtils.cleanupAll _).expects(*,*).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, cleanupUtils)

    pipe.doPreReadSteps() match {
      case Left(err) => assert(err.getError match {
        case ExportFromVerticaError(_) => true
        case _ => false
      })
      case Right(_) => fail
    }
  }

  // Default partition count of 1 per file, with equal partition counts
  it should "return partitioning info from pre-read steps based on files from filesystem" in {
    val config = makeReadConfig

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createDir _).expects(*,*).returning(Right())
    (fileStoreLayer.fileExists _).expects(*).returning(Right(false)).anyNumberOfTimes()

    // Files returned by filesystem (mock of what vertica would create
    val exportedFiles = Array[String](expectedAddress+"/t1p1.parquet", expectedAddress+"/t1p2.parquet", expectedAddress+"/t1p3.parquet")
    (fileStoreLayer.getFileList _).expects(expectedAddress).returning(Right(exportedFiles))

    for (file <- exportedFiles) {
      (fileStoreLayer.getParquetFileMetadata _).expects(file).returning(Right(ParquetFileMetadata(file, 4)))
    }

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*, *).returning(Right())
    (jdbcLayer.close _).expects().returning(Right())

    val columnDef = ColumnDef("col1", java.sql.Types.REAL, "REAL", 32, 32, signed = false, nullable = true, metadata)

    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    pipe.doPreReadSteps() match {
      case Left(_) => fail
      case Right(partitionInfo) =>
        val partitions = partitionInfo.partitionSeq
        assert(partitions.length == 3)
        for (p <- partitions) {
          p match {
            case vp: VerticaDistributedFilesystemPartition =>
              assert(vp.fileRanges.size == 1)
              assert(vp.fileRanges.head.minRowGroup == 0)
              assert(vp.fileRanges.head.maxRowGroup == 3)
            case _ => fail
          }
        }
    }
  }

  // Default partition count of 1 per file, with equal partition counts
  it should "split a file evenly among many partitions" in {
    // 2 row groups per partition except last one which should have 1 row group
    val partitionCount = 15
    val rowGroupCount = 29

    val config = makeReadConfig.copy(partitionCount = Some(partitionCount))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createDir _).expects(*,*).returning(Right())
    (fileStoreLayer.fileExists _).expects(*).returning(Right(false)).anyNumberOfTimes()

    // Files returned by filesystem (mock of what vertica would create
    val exportedFiles = Array[String](expectedAddress+"/t1p1.parquet")
    (fileStoreLayer.getFileList _).expects(expectedAddress).returning(Right(exportedFiles))

    for (file <- exportedFiles) {
      (fileStoreLayer.getParquetFileMetadata _).expects(file).returning(Right(ParquetFileMetadata(file, rowGroupCount)))
    }

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*, *).returning(Right())
    (jdbcLayer.close _).expects().returning(Right())

    val columnDef = ColumnDef("col1", java.sql.Types.REAL, "REAL", 32, 32, signed = false, nullable = true, metadata)

    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    pipe.doPreReadSteps() match {
      case Left(_) => fail
      case Right(partitionInfo) =>
        val partitions = partitionInfo.partitionSeq
        assert(partitions.length == 15)
        var i = 0
        for (p <- partitions) {
          p match {
            case vp: VerticaDistributedFilesystemPartition =>
              assert(vp.fileRanges.size == 1)
              if(i != 14) {
                assert(vp.fileRanges.head.minRowGroup == i*2)
                assert(vp.fileRanges.head.maxRowGroup == (i*2)+1)
              }
              else {
                assert(vp.fileRanges.head.minRowGroup == 28)
                assert(vp.fileRanges.head.maxRowGroup == 28)
              }
            case _ => fail
          }
          i += 1
        }
    }
  }

  it should "Split up files among partitions when they don't divide evenly" in {
    val partitionCount = 4
    val rowGroupPerFile = 5
    // With 3 files so 15 total row groups, partitions should end up containning rows as such: [4,4,4,3]

    val config = makeReadConfig.copy(partitionCount = Some(partitionCount))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createDir _).expects(*,*).returning(Right())
    (fileStoreLayer.fileExists _).expects(*).returning(Right(false)).anyNumberOfTimes()

    // Files returned by filesystem (mock of what vertica would create
    val fname1 = expectedAddress+"/t1p1.parquet"
    val fname2 = expectedAddress+"/t1p2.parquet"
    val fname3 = expectedAddress+"/t1p3.parquet"
    val exportedFiles = Array[String](fname1, fname2 , fname3)
    (fileStoreLayer.getFileList _).expects(expectedAddress).returning(Right(exportedFiles))

    for (file <- exportedFiles) {
      (fileStoreLayer.getParquetFileMetadata _).expects(file).returning(Right(ParquetFileMetadata(file, rowGroupPerFile)))
    }

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*, *).returning(Right())
    (jdbcLayer.close _).expects().returning(Right(()))

    val columnDef = ColumnDef("col1", java.sql.Types.REAL, "REAL", 32, 32, signed = false, nullable = true, metadata)

    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    pipe.doPreReadSteps() match {
      case Left(_) => fail
      case Right(partitionInfo) =>
        val partitions = partitionInfo.partitionSeq
        assert(partitions.length == partitionCount)
        assert(partitions(0).asInstanceOf[VerticaDistributedFilesystemPartition].fileRanges(0) == ParquetFileRange(fname1,0,3,Some(0)))
        assert(partitions(1).asInstanceOf[VerticaDistributedFilesystemPartition].fileRanges(0) == ParquetFileRange(fname1,4,4,Some(1)))
        assert(partitions(1).asInstanceOf[VerticaDistributedFilesystemPartition].fileRanges(1) == ParquetFileRange(fname2,0,2,Some(0)))
        assert(partitions(2).asInstanceOf[VerticaDistributedFilesystemPartition].fileRanges(0) == ParquetFileRange(fname2,3,4,Some(1)))
        assert(partitions(2).asInstanceOf[VerticaDistributedFilesystemPartition].fileRanges(1) == ParquetFileRange(fname3,0,1,Some(0)))
        assert(partitions(3).asInstanceOf[VerticaDistributedFilesystemPartition].fileRanges(0) == ParquetFileRange(fname3,2,4,Some(1)))
    }
  }

  it should "Construct file range count map and pass include it in partitions" in {
    val partitionCount = 4
    val rowGroupPerFile = 5
    // With 3 files so 15 total row groups, partitions should end up containning rows as such: [4,4,4,3]

    val config = makeReadConfig.copy(partitionCount = Some(partitionCount))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createDir _).expects(*,*).returning(Right())
    (fileStoreLayer.fileExists _).expects(*).returning(Right(false)).anyNumberOfTimes()

    // Files returned by filesystem (mock of what vertica would create
    val fname1 = expectedAddress+"/t1p1.parquet"
    val fname2 = expectedAddress+"/t1p2.parquet"
    val fname3 = expectedAddress+"/t1p3.parquet"
    val exportedFiles = Array[String](fname1, fname2 , fname3)
    (fileStoreLayer.getFileList _).expects(expectedAddress).returning(Right(exportedFiles))

    for (file <- exportedFiles) {
      (fileStoreLayer.getParquetFileMetadata _).expects(file).returning(Right(ParquetFileMetadata(file, rowGroupPerFile)))
    }

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*, *).returning(Right())
    (jdbcLayer.close _).expects().returning(Right(()))

    val columnDef = ColumnDef("col1", java.sql.Types.REAL, "REAL", 32, 32, signed = false, nullable = true, metadata)

    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    pipe.doPreReadSteps() match {
      case Left(_) => fail
      case Right(partitionInfo) =>
        val partitions = partitionInfo.partitionSeq
        assert(partitions.length == partitionCount)
        for(partition <- partitions){
          partition.asInstanceOf[VerticaDistributedFilesystemPartition].rangeCountMap match {
            case None => fail
            case Some(map) =>
              assert(map(fname1) == 2)
              assert(map(fname2) == 2)
              assert(map(fname3) == 2)
          }
        }
    }
  }

  it should "Return an error when there is a problem retrieving file list" in {
    val config = makeReadConfig

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createDir _).expects(*,*).returning(Right())
    (fileStoreLayer.fileExists _).expects(*).returning(Right(false)).anyNumberOfTimes()

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*, *).returning(Right(()))
    (jdbcLayer.close _).expects().returning(Right(()))

    val columnDef = ColumnDef("col1", java.sql.Types.REAL, "REAL", 32, 32, signed = false, nullable = true, metadata)

    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")

    val cleanupUtils = mock[CleanupUtilsInterface]
    (cleanupUtils.cleanupAll _).expects(*,*).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, cleanupUtils)

    (fileStoreLayer.getFileList _).expects(*).returning(Left(ParentDirMissingError("")))

    pipe.doPreReadSteps() match {
      case Left(err) => assert(err.getError == ParentDirMissingError(""))
      case Right(_) => fail
    }
  }

  it should "Return an error when there are no files to create partitions from" in {
    val config = makeReadConfig

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createDir _).expects(*,*).returning(Right())
    (fileStoreLayer.fileExists _).expects(*).returning(Right(false)).anyNumberOfTimes()

    // Files returned by filesystem (mock of what vertica would create
    val exportedFiles = Array[String]()
    (fileStoreLayer.getFileList _).expects(*).returning(Right(exportedFiles))

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*, *).returning(Right())
    (jdbcLayer.close _).expects().returning(Right(()))

    val columnDef = ColumnDef("col1", java.sql.Types.REAL, "REAL", 32, 32, signed = false, nullable = true, metadata)

    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")

    val cleanupUtils = mock[CleanupUtilsInterface]
    (cleanupUtils.cleanupAll _).expects(*,*).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, cleanupUtils)

    pipe.doPreReadSteps() match {
      case Left(err) => assert(err.getError == FileListEmptyPartitioningError())
      case Right(_) => fail
    }
  }

  it should "Use filestore layer to read data" in {
    val config = makeReadConfig

    val filename = "test.parquet"
    val v1: Int = 1
    val v2: Float = 2.0f
    val data = DataBlock(List(InternalRow(v1, v2) ))

    val fileRange = ParquetFileRange(filename, 0, 1)
    val partition = VerticaDistributedFilesystemPartition(List(fileRange))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.openReadParquetFile _).expects(fileRange).returning(Right())
    (fileStoreLayer.readDataFromParquetFile _).expects(*).returning(Right(data))
    (fileStoreLayer.closeReadParquetFile _).expects().returning(Right())

    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface], mock[CleanupUtilsInterface], dataSize = 2)

    this.failOnError(pipe.startPartitionRead(partition))

    pipe.readData match {
      case Left(_) => fail
      case Right(data) =>
        assert(data.data.size == 1)
        assert(data.data(0).getInt(0) == v1)
        assert(data.data(0).getFloat(1) == v2)
    }

    this.failOnError(pipe.endPartitionRead())
  }

  it should "Use filestore layer to read from multiple files" in {
    val config = makeReadConfig

    val v1: Int = 1
    val v2: Float = 2.0f
    val v3: Int = 2
    val data1 = DataBlock(List(InternalRow(v1, v2) ))
    val data2 = DataBlock(List(InternalRow(v3, v2) ))
    val emptyData = DataBlock(List())

    val filename1 = "test.parquet"
    val filename2 = "test2.parquet"
    val fileRange1 = ParquetFileRange(filename1, 0, 1)
    val fileRange2 = ParquetFileRange(filename2, 0, 1)
    val partition = VerticaDistributedFilesystemPartition(List(fileRange1, fileRange2))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.openReadParquetFile _).expects(fileRange1).returning(Right())
    (fileStoreLayer.readDataFromParquetFile _).expects(*).returning(Right(data1))
    (fileStoreLayer.readDataFromParquetFile _).expects(*).returning(Right(emptyData))
    (fileStoreLayer.closeReadParquetFile _).expects().returning(Right())
    (fileStoreLayer.openReadParquetFile _).expects(fileRange2).returning(Right())
    (fileStoreLayer.readDataFromParquetFile _).expects(*).returning(Right(data2))
    (fileStoreLayer.readDataFromParquetFile _).expects(*).returning(Right(emptyData))
    (fileStoreLayer.closeReadParquetFile _).expects().returning(Right())

    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface], mock[CleanupUtilsInterface], dataSize = 2)

    pipe.startPartitionRead(partition) match {
      case Left(_) => fail
      case Right(_) => ()
    }

    pipe.readData match {
      case Left(_) => fail
      case Right(data) =>
        assert(data.data.size == 1)
        assert(data.data(0).getInt(0) == v1)
        assert(data.data(0).getFloat(1) == v2)
    }

    pipe.readData match {
      case Left(_) => fail
      case Right(data) =>
        assert(data.data.size == 1)
        assert(data.data(0).getInt(0) == v3)
        assert(data.data(0).getFloat(1) == v2)
    }

    pipe.readData match {
      case Left(err) => fail(err.getFullContext)
      case Right(data) =>
        assert(data.data.isEmpty)
    }

    this.failOnError(pipe.endPartitionRead())
  }

  it should "Call cleanup when done reading files" in {
    val config = makeReadConfig

    val v1: Int = 1
    val v2: Float = 2.0f
    val v3: Int = 2
    val data1 = DataBlock(List(InternalRow(v1, v2) ))
    val data2 = DataBlock(List(InternalRow(v3, v2) ))
    val emptyData = DataBlock(List())

    val filename1 = "test.parquet"
    val filename2 = "test2.parquet"
    val fileRange1 = ParquetFileRange(filename1, 0, 1, Some(0))
    val fileRange2 = ParquetFileRange(filename2, 0, 1, Some(0))
    val partition = VerticaDistributedFilesystemPartition(List(fileRange1, fileRange2), Some(Map(filename1 -> 1, filename2 -> 1)))

    val jdbcLayer = mock[JdbcLayerInterface]

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.openReadParquetFile _).expects(fileRange1).returning(Right())
    (fileStoreLayer.readDataFromParquetFile _).expects(*).returning(Right(data1))
    (fileStoreLayer.readDataFromParquetFile _).expects(*).returning(Right(emptyData))
    (fileStoreLayer.closeReadParquetFile _).expects().returning(Right())
    (fileStoreLayer.openReadParquetFile _).expects(fileRange2).returning(Right())
    (fileStoreLayer.readDataFromParquetFile _).expects(*).returning(Right(data2))
    (fileStoreLayer.readDataFromParquetFile _).expects(*).returning(Right(emptyData))
    (fileStoreLayer.closeReadParquetFile _).expects().returning(Right())

    // Should be called to clean up 2 files
    val cleanupUtils = mock[CleanupUtilsInterface]
    (cleanupUtils.checkAndCleanup _).expects(fileStoreLayer, FileCleanupInfo(filename1,0,1)).returning(Right(()))
    (cleanupUtils.checkAndCleanup _).expects(fileStoreLayer, FileCleanupInfo(filename2,0,1)).returning(Right(()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface], cleanupUtils, dataSize = 2)

    pipe.startPartitionRead(partition)
    pipe.readData
    pipe.readData
    pipe.readData
    pipe.endPartitionRead()
  }

  it should "Return an error if there is a partition type mismatch" in {
    val config = makeReadConfig

    val partition = mock[VerticaPartition]

    val fileStoreLayer = mock[FileStoreLayerInterface]
    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface], mock[CleanupUtilsInterface])

    pipe.startPartitionRead(partition) match {
      case Left(err) => assert(err.getError == InvalidPartition())
      case Right(_) => fail
    }
  }

  it should "Pass on errors from filestore layer on read start" in {
    val config = makeReadConfig

    val filename = "test.parquet"
    val partition = VerticaDistributedFilesystemPartition(List(ParquetFileRange(filename, 0, 1)))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.openReadParquetFile _).expects(*).returning(Left(StagingFsUrlMissingError()))

    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface], mock[CleanupUtilsInterface])

    pipe.startPartitionRead(partition) match {
      case Left(err) => assert(err.getError == StagingFsUrlMissingError())
      case Right(_) => fail
    }
  }

  it should "Pass on errors from filestore layer on read" in {
    val config = makeReadConfig

    val filename = "test.parquet"
    val partition = VerticaDistributedFilesystemPartition(List(ParquetFileRange(filename, 0, 1)))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.openReadParquetFile _).expects(*).returning(Right())
    (fileStoreLayer.readDataFromParquetFile _).expects(*).returning(Left(StagingFsUrlMissingError()))

    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface], mock[CleanupUtilsInterface])

    this.failOnError(pipe.startPartitionRead(partition))

    pipe.readData match {
      case Left(err) => assert(err.getError == StagingFsUrlMissingError())
      case Right(_) => fail
    }
  }

  it should "Pass on errors from filestore layer on read end" in {
    val config = makeReadConfig

    val filename = "test.parquet"
    val v1: Int = 1
    val v2: Float = 2.0f
    val data = DataBlock(List(InternalRow(v1, v2) ))

    val partition = VerticaDistributedFilesystemPartition(List(ParquetFileRange(filename, 0, 1)))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.openReadParquetFile _).expects(*).returning(Right())
    (fileStoreLayer.readDataFromParquetFile _).expects(*).returning(Right(data))
    (fileStoreLayer.closeReadParquetFile _).expects().returning(Left(StagingFsUrlMissingError()))

    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface], mock[CleanupUtilsInterface])

    this.failOnError(pipe.startPartitionRead(partition))
    this.failOnError(pipe.readData)

    pipe.endPartitionRead() match {
      case Left(err) => assert(err.getError == StagingFsUrlMissingError())
      case Right(_) => fail
    }
  }

  it should "cast TIMEs to strings" in {
    val config = this.makeReadConfig
    val fileStoreLayer = this.mockFileStoreLayer(config)
    val expectedJdbcCommand = "EXPORT TO PARQUET(directory = 'hdfs://example-hdfs:8020/tmp/test/dummy', fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT col1::varchar AS col1 FROM \"dummy\";"
    val jdbcLayer = this.mockJdbcLayer(expectedJdbcCommand)

    val columnDef = ColumnDef("col1", java.sql.Types.TIME, "TIME", size, scale, signed = false, nullable = true, metadata)
    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1::varchar AS col1")

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])
    this.failOnError(pipe.doPreReadSteps())
  }

  it should "cast UUIDs to strings" in {
    val config = this.makeReadConfig
    val fileStoreLayer = this.mockFileStoreLayer(config)
    val expectedJdbcCommand = "EXPORT TO PARQUET(directory = 'hdfs://example-hdfs:8020/tmp/test/dummy', fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT col1::varchar AS col1 FROM \"dummy\";"
    val jdbcLayer = this.mockJdbcLayer(expectedJdbcCommand)
    val columnDef = ColumnDef("col1", java.sql.Types.OTHER, "UUID", size, scale, signed = false, nullable = true, metadata)
    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1::varchar AS col1")

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    this.failOnError(pipe.doPreReadSteps())
  }

  it should "not push down an empty list of filters" in {
    val config = this.makeReadConfig
    val fileStoreLayer = this.mockFileStoreLayer(config)
    val expectedJdbcCommand = "EXPORT TO PARQUET(directory = 'hdfs://example-hdfs:8020/tmp/test/dummy', fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT col1 FROM \"dummy\";"
    val jdbcLayer = this.mockJdbcLayer(expectedJdbcCommand)

    val columnDef = makeIntColumnDef
    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])
    this.failOnError(pipe.doPreReadSteps())
  }

  it should "push down a filter" in {
    val config = this.makeReadConfig
    val fileStoreLayer = this.mockFileStoreLayer(config)
    val expectedJdbcCommand = "EXPORT TO PARQUET(directory = 'hdfs://example-hdfs:8020/tmp/test/dummy', " +
      "fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT col1 FROM \"dummy\" WHERE (\"col1\" = 2);"
    val jdbcLayer = this.mockJdbcLayer(expectedJdbcCommand)

    val columnDef = this.makeIntColumnDef
    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")

    config.setPushdownFilters(List(
      PushFilter(EqualTo("col1", 2), "(\"col1\" = 2)")))

    val pipe = new VerticaDistributedFilesystemReadPipe(
      config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    this.failOnError(pipe.doPreReadSteps())
  }

  it should "push down multiple filters" in {
    val config = this.makeReadConfig
    val fileStoreLayer = this.mockFileStoreLayer(config)
    val lowerBound = 2
    val upperBound = 6
    val jdbcLayer = this.mockJdbcLayer("EXPORT TO PARQUET(" +
      "directory = 'hdfs://example-hdfs:8020/tmp/test/dummy', " +
      "fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT col1 FROM \"dummy\" " +
      "WHERE (\"col1\" < " + upperBound + ") AND (\"col1\" > " + lowerBound + ");")

    val columnDef = this.makeIntColumnDef
    val mockSchemaTools = this.mockSchemaTools(List(columnDef), "col1")
    val pipe = new VerticaDistributedFilesystemReadPipe(
      config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    config.setPushdownFilters(List(
      PushFilter(LessThan("col1", upperBound), "(\"col1\" < " + upperBound + ")"),
      PushFilter(GreaterThan("col1", lowerBound), "(\"col1\" > " + lowerBound + ")")))

    this.failOnError(pipe.doPreReadSteps())
  }

  it should "push down a column projection" in {
    val config = this.makeReadConfig
    val fileStoreLayer = this.mockFileStoreLayer(config)
    val expectedJdbcCommand = "EXPORT TO PARQUET(directory = 'hdfs://example-hdfs:8020/tmp/test/dummy', " +
      "fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT col1 FROM \"dummy\";"
    val jdbcLayer = this.mockJdbcLayer(expectedJdbcCommand)

    val columnDef1 = ColumnDef("col1", java.sql.Types.INTEGER, "INT", size, scale, signed = false, nullable = true, metadata)
    val columnDef2 = ColumnDef("col2", java.sql.Types.INTEGER, "INT", size, scale, signed = false, nullable = true, metadata)
    val requiredSchema = StructType(StructField("col1", LongType) :: Nil)
    val mockSchemaTools = this.mockSchemaTools(List(columnDef1, columnDef2), "col1", requiredSchema)
    val pipe = new VerticaDistributedFilesystemReadPipe(
      config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    config.setRequiredSchema(requiredSchema)
    this.failOnError(pipe.doPreReadSteps())
  }

  it should "push down multiple column projections" in {
    val config = this.makeReadConfig
    val fileStoreLayer = this.mockFileStoreLayer(config)
    val jdbcLayer = this.mockJdbcLayer("EXPORT TO PARQUET(" +
      "directory = 'hdfs://example-hdfs:8020/tmp/test/dummy', " +
      "fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT col1,col3 FROM \"dummy\";")

    val columnDef1 = ColumnDef("col1", java.sql.Types.INTEGER, "INT", size, scale, signed = false, nullable = true, metadata)
    val columnDef2 = ColumnDef("col2", java.sql.Types.INTEGER, "INT", size, scale, signed = false, nullable = true, metadata)
    val columnDef3 = ColumnDef("col3", java.sql.Types.VARCHAR, "STRING", size, scale, signed = false, nullable = true, metadata)
    val requiredSchema = StructType(
      StructField("col1", LongType) ::
      StructField("col3", StringType) ::
      Nil)

    val mockSchemaTools = this.mockSchemaTools(
      List(columnDef1, columnDef2, columnDef3), "col1,col3", requiredSchema)

    val pipe = new VerticaDistributedFilesystemReadPipe(
      config, fileStoreLayer, jdbcLayer, mockSchemaTools, mock[CleanupUtilsInterface])

    config.setRequiredSchema(StructType(
      StructField("col1", LongType) ::
      StructField("col3", StringType) ::
      Nil))

    this.failOnError(pipe.doPreReadSteps())
  }

  it should "not push down column projections when there is no required schema" in {
    val config = this.makeReadConfig
    val fileStoreLayer = this.mockFileStoreLayer(config)
    val jdbcLayer = this.mockJdbcLayer("EXPORT TO PARQUET(" +
      "directory = 'hdfs://example-hdfs:8020/tmp/test/dummy', " +
      "fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT col1,col2,col3 FROM \"dummy\";")

    val columnDef1 = ColumnDef("col1", java.sql.Types.INTEGER, "INT", size, scale, signed = false, nullable = true, metadata)
    val columnDef2 = ColumnDef("col2", java.sql.Types.INTEGER, "INT", size, scale, signed = false, nullable = true, metadata)
    val columnDef3 = ColumnDef("col3", java.sql.Types.VARCHAR, "STRING", size, scale, signed = false, nullable = true, metadata)
    val schemaTools = this.mockSchemaTools(List(columnDef1, columnDef2, columnDef3), "col1,col2,col3")
    val pipe = new VerticaDistributedFilesystemReadPipe(
      config, fileStoreLayer, jdbcLayer, schemaTools, mock[CleanupUtilsInterface])

    config.setRequiredSchema(StructType(Nil))
    this.failOnError(pipe.doPreReadSteps())
  }
}
