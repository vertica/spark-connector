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
  def apply(file: PartitionedFile, partitionIndex: Int): VerticaPartitionedFile =
    new VerticaPartitionedFile(file.partitionValues, file.filePath, file.start, file.length, partitionIndex)
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
/**
 * Wraps a [[JsonTable]] so that that it will create a [[VerticaScanWrapperBuilder]].
 * */
class VerticaJsonTable(val jsonTable: JsonTable) extends Table with SupportsRead {
  override def name(): String = jsonTable.name

  override def schema(): StructType = jsonTable.schema

  override def capabilities(): util.Set[TableCapability] = jsonTable.capabilities()

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder =
    new VerticaScanWrapperBuilder(jsonTable.newScanBuilder(caseInsensitiveStringMap))
}

/**
 * Wraps a [[ScanBuilder]] to create a [[VerticaScanWrapper]]
 * */
class VerticaScanWrapperBuilder(val builder: ScanBuilder) extends ScanBuilder {
  override def build(): Scan = new VerticaScanWrapper(builder.build())
}

/**
 * Wraps a [[Scan]] so that it will create a [[PartitionReaderWrapperFactory]]
 *
 * planInputPartition() will also record partitioning information.
 * */
class VerticaScanWrapper(val scan: Scan) extends Scan with Batch {
  override def readSchema(): StructType = scan.readSchema()

  /**
   * Calls the wrapped scan to plan inputs. Then process them into [[VerticaFilePartition]] with partitioning info
   * */
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

  override def createReaderFactory(): PartitionReaderFactory = new PartitionReaderWrapperFactory(scan.toBatch.createReaderFactory())

  override def toBatch: Batch = this
}

/**
 * Wraps a [[PartitionReaderFactory]] so it will create a [[PartitionReaderWrapper]]
 *
 * planInputPartition() will also record partitioning information.
 * */
class PartitionReaderWrapperFactory(val readerFactory: PartitionReaderFactory) extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] =
    new PartitionReaderWrapper(readerFactory.createReader(inputPartition), inputPartition)
}

/**
 * Wraps a [[PartitionReader]], allowing us to intercept it's methods and add additional functionalities.
 * */
class PartitionReaderWrapper(val reader: PartitionReader[InternalRow], val partitions: InputPartition) extends PartitionReader[InternalRow] {
  private val logger = LogProvider.getLogger(classOf[PartitionReaderWrapper])

  override def next(): Boolean = reader.next()

  override def get(): InternalRow = reader.get()

  override def close(): Unit = {
    reader.close()
    //  Todo: Clean up files here.
    logger.info("Cleaning up")
  }
}
