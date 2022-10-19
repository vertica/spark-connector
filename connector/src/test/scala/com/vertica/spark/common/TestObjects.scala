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

package com.vertica.spark.common

import com.vertica.spark.config.{AWSOptions, BasicJdbcAuth, DistributedFilesystemReadConfig, DistributedFilesystemWriteConfig, FileStoreConfig, GCSOptions, JDBCConfig, JDBCTLSConfig, TableName, ValidFilePermissions, VerticaMetadata}
import com.vertica.spark.datasource.core.{DataBlock, PartitionInfo, Require, VerticaPartition, VerticaPipeInterface, VerticaPipeReadInterface, VerticaPipeWriteInterface}
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.spark.sql.types.StructType

object TestObjects {
  val fileStoreConfig: FileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/", "test", false, AWSOptions(None, None, None, None, None, None, None), GCSOptions(None, None, None))
  val tablename: TableName = TableName("testtable", None)
  val jdbcConfig: JDBCConfig = JDBCConfig(
    "1.1.1.1", 1234, "test", BasicJdbcAuth("test", "test"), JDBCTLSConfig(tlsMode = Require, None, None, None, None))
  val writeConfig: DistributedFilesystemWriteConfig = DistributedFilesystemWriteConfig(
    jdbcConfig = jdbcConfig,
    fileStoreConfig = fileStoreConfig,
    tablename = tablename,
    schema = new StructType(),
    targetTableSql = None,
    strlen = 1024,
    copyColumnList = None,
    sessionId = "id",
    failedRowPercentTolerance = 0.0f,
    filePermissions = ValidFilePermissions("777").getOrElse(throw new Exception("File perm error")),
    createExternalTable = None,
    saveJobStatusTable = false,
    truncate = false
  )

  val readConfig: DistributedFilesystemReadConfig = DistributedFilesystemReadConfig(
    jdbcConfig = jdbcConfig,
    fileStoreConfig = fileStoreConfig,
    tableSource = tablename,
    partitionCount = None,
    metadata = None,
    filePermissions = ValidFilePermissions("777").getOrElse(throw new Exception("File perm error")),
    maxRowGroupSize = 64,
    maxFileSize = 512)

  /**
   * Dummy class to instantiate mock object
   * */
  class TestWritePipe extends VerticaPipeWriteInterface with VerticaPipeInterface {
    /**
     * Initial setup for the whole write operation. Called by driver.
     */
    override def doPreWriteSteps(): ConnectorResult[Unit] = ???

    /**
     * Initial setup for the write of an individual partition. Called by executor.
     *
     * @param uniqueId Unique identifier for the partition being written
     */
    override def startPartitionWrite(uniqueId: String): ConnectorResult[Unit] = ???

    /**
     * Write a block of data to the underlying source. Called by executor.
     */
    override def writeData(data: DataBlock): ConnectorResult[Unit] = ???

    /**
     * Ends the write, doing any necessary cleanup. Called by executor once writing of the given partition is done.
     */
    override def endPartitionWrite(): ConnectorResult[Unit] = ???

    /**
     * Commits the data being written. Called by the driver once all executors have succeeded writing.
     */
    override def commit(): ConnectorResult[Unit] = ???

    /**
     * Retrieve any needed metadata for a table needed to inform the configuration of the operation.
     *
     * Can include schema and things like node information / segmentation -- should have caching mechanism
     */
    override def getMetadata: ConnectorResult[VerticaMetadata] = ???

    /**
     * Returns the default number of rows to read/write from this pipe at a time.
     */
    override def getDataBlockSize: ConnectorResult[Long] = ???
  }

  /**
   * Dummy class to instantiate mock object
   * */
  class TestReadPipe extends VerticaPipeReadInterface with VerticaPipeInterface {
    /**
     * Initial setup for the whole read operation. Called by driver.
     *
     * @return Partitioning information for how the read operation will be partitioned across spark nodes
     */
    override def doPreReadSteps(): ConnectorResult[PartitionInfo] = ???

    /**
     * Initial setup for the read of an individual partition. Called by executor.
     */
    override def startPartitionRead(partition: VerticaPartition): ConnectorResult[Unit] = ???

    /**
     * Reads a block of data to the underlying source. Called by executor.
     */
    override def readData: ConnectorResult[DataBlock] = ???

    /**
     * Ends the read, doing any necessary cleanup. Called by executor once reading the partition is done.
     */
    override def endPartitionRead(): ConnectorResult[Unit] = ???

    /**
     * Retrieve any needed metadata for a table needed to inform the configuration of the operation.
     *
     * Can include schema and things like node information / segmentation -- should have caching mechanism
     */
    override def getMetadata: ConnectorResult[VerticaMetadata] = ???

    /**
     * Returns the default number of rows to read/write from this pipe at a time.
     */
    override def getDataBlockSize: ConnectorResult[Long] = ???
  }

}
