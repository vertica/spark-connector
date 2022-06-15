package com.vertica.spark.datasource.wrappers

import com.vertica.spark.common.TestObjects
import com.vertica.spark.datasource.partitions.file.VerticaFilePartition
import com.vertica.spark.util.cleanup.{CleanupUtils, DistributedFilesCleaner}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class PartitionReaderWrapperTest extends AnyFlatSpec with MockFactory{

  behavior of "PartitionReaderWrapperTest"

  private val config = TestObjects.readConfig

  it should "get" in {
    val reader = mock[PartitionReader[InternalRow]]
    (reader.get _).expects().returning(mock[InternalRow])
    val mockCleanupUtils = new CleanupUtils
    val mockCleaner = new DistributedFilesCleaner(config, mockCleanupUtils)

    new PartitionReaderWrapper(reader, mock[VerticaFilePartition], mockCleaner).get()
  }

  it should "next" in {
    val reader = mock[PartitionReader[InternalRow]]
    (reader.next _).expects()
    val mockCleanupUtils = new CleanupUtils
    val mockCleaner = new DistributedFilesCleaner(config, mockCleanupUtils)

    new PartitionReaderWrapper(reader, mock[VerticaFilePartition], mockCleaner).next()
  }

  it should "perform cleanup on close" in {
    val reader = mock[PartitionReader[InternalRow]]
    (reader.close _).expects()
    val partitions = mock[VerticaFilePartition]
    (partitions.getFilePortions _).expects().returning(Seq())
    (partitions.getFilePortions _).expects().returning(Seq())

    val mockCleanupUtils = new CleanupUtils
    val mockCleaner = new DistributedFilesCleaner(config, mockCleanupUtils)
    new PartitionReaderWrapper(reader, partitions, mockCleaner).close()
  }
}
