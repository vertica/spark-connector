package com.vertica.spark.json

import com.vertica.spark.config.LogProvider
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.json.{JsonPartitionReaderFactory, JsonScanBuilder, JsonTable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util


class VerticaJsonReader(reader: PartitionReader[InternalRow]) extends PartitionReader[InternalRow] {
  private val logger = LogProvider.getLogger(classOf[VerticaJsonReader])

  override def next(): Boolean = reader.next()

  override def get(): InternalRow = reader.get()

  override def close(): Unit = {
    reader.close()
    //  Todo: Clean up files here.
    logger.info("Cleaning up")
  }
}

class VerticaJsonReaderFactory(readerFactory: PartitionReaderFactory) extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] =
    new VerticaJsonReader(readerFactory.createReader(inputPartition))
}

class VerticaJsonScan(jsonScan: Scan) extends Scan with Batch {
  override def readSchema(): StructType = jsonScan.readSchema()

  override def planInputPartitions(): Array[InputPartition] = jsonScan.toBatch.planInputPartitions()

  override def createReaderFactory(): PartitionReaderFactory = new VerticaJsonReaderFactory(jsonScan.toBatch.createReaderFactory())

  override def toBatch: Batch = this
}

class VerticaJsonScanBuilder(builder: JsonScanBuilder) extends ScanBuilder{
  override def build(): Scan = new VerticaJsonScan(builder.build())
}


class VerticaJsonTable(jsonTable: JsonTable) extends Table with SupportsRead {
  override def name(): String = jsonTable.name

  override def schema(): StructType = jsonTable.schema

  override def capabilities(): util.Set[TableCapability] = jsonTable.capabilities()

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder =
    new VerticaJsonScanBuilder(jsonTable.newScanBuilder(caseInsensitiveStringMap))
}