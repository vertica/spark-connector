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

package com.vertica.spark.json

import com.vertica.spark.config.LogProvider
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.json.{JsonScanBuilder, JsonTable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter

object VerticaPartitionedFile {
  def apply(file: PartitionedFile, partitionIndex: Int) = new VerticaPartitionedFile(file.partitionValues, file.filePath, file.start, file.length, partitionIndex)
}

/**
 * Extended from Spark's PartitionedFile with the purpose to hold extra partitioning information.
 *
 * @param partitionIndex The partition's id number out of all partitions created from a file.
 * */
class VerticaPartitionedFile(override val partitionValues: InternalRow,
                             override val filePath: String,
                             override val start: Long,
                             override val length: Long,
                             val partitionIndex: Int
                            )
  extends PartitionedFile(partitionValues, filePath, start, length)

/**
 * Extended from Spark's FilePartition to hold extra to hold extra partitioning data.
 *
 * @param partitioningRecords A record of the partition count for all file partition created with the key being the
 *                            file path.
 * */
class VerticaFilePartition(override val index: Int,
                           override val files: Array[PartitionedFile],
                           val partitioningRecords: Map[String, Int])
  extends FilePartition(index, files)

object VerticaJsonScanSupport {
  def getJsonScan(filePath: String, schema: Option[StructType], sparkSession: SparkSession): Scan = {
    val paths = List(filePath)
    val options = CaseInsensitiveStringMap.empty()
    val fallback = classOf[JsonFileFormat]
    val jsonTable = JsonTable("Vertica Table", sparkSession, options , paths, schema, fallback)
    val verticaJsonTable = new VerticaJsonTable(jsonTable)
    val builderOpts = new CaseInsensitiveStringMap(Map[String, String]().asJava)
    verticaJsonTable.newScanBuilder(builderOpts).build()
  }
}

class VerticaJsonTable(jsonTable: JsonTable) extends Table with SupportsRead {
  override def name(): String = jsonTable.name

  override def schema(): StructType = jsonTable.schema

  override def capabilities(): util.Set[TableCapability] = jsonTable.capabilities()

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder =
    new VerticaScanWrapperBuilder(jsonTable.newScanBuilder(caseInsensitiveStringMap))
}

class VerticaScanWrapperBuilder(builder: JsonScanBuilder) extends ScanBuilder {
  override def build(): Scan = new VerticaScanWrapper(builder.build())
}

class VerticaScanWrapper(scan: Scan) extends Scan with Batch {
  override def readSchema(): StructType = scan.readSchema()

  override def planInputPartitions(): Array[InputPartition] = {
    val partitioningRecord = scala.collection.mutable.Map[String, Int]()
    def makeVerticaPartitionedFile(file: PartitionedFile) = {
      val key = file.filePath
      val count = partitioningRecord.getOrElse(key, 0)
      partitioningRecord.put(key, count + 1)
      VerticaPartitionedFile(file, count)
    }

    scan.toBatch.planInputPartitions()
      .map(partition => partition.asInstanceOf[FilePartition])
      .map(filePartition =>
        filePartition.copy(files = filePartition.files.map(makeVerticaPartitionedFile)))
      .map(partition => new VerticaFilePartition(partition.index, partition.files, partitioningRecord.toMap))
  }

  override def createReaderFactory(): PartitionReaderFactory = new PartitionReaderFactoryWrapper(scan.toBatch.createReaderFactory())

  override def toBatch: Batch = this
}

class PartitionReaderFactoryWrapper(readerFactory: PartitionReaderFactory) extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] =
    new PartitionReaderWrapper(readerFactory.createReader(inputPartition), inputPartition)
}

class PartitionReaderWrapper(reader: PartitionReader[InternalRow], partitions: InputPartition) extends PartitionReader[InternalRow] {
  private val logger = LogProvider.getLogger(classOf[PartitionReaderWrapper])

  override def next(): Boolean = reader.next()

  override def get(): InternalRow = reader.get()

  override def close(): Unit = {
    reader.close()
    //  Todo: Clean up files here.
    logger.info("Cleaning up")
  }
}