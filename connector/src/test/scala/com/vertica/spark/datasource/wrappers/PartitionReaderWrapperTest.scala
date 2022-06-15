package com.vertica.spark.datasource.wrappers

import com.vertica.spark.json.VerticaFilePartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class PartitionReaderWrapperTest extends AnyFlatSpec with MockFactory{

  behavior of "PartitionReaderWrapperTest"

  it should "get" in {
    val reader = mock[PartitionReader[InternalRow]]
    (reader.get _).expects().returning(mock[InternalRow])
    new PartitionReaderWrapper(reader, mock[VerticaFilePartition]).get()
  }

  it should "next" in {
    val reader = mock[PartitionReader[InternalRow]]
    (reader.next _).expects()
    new PartitionReaderWrapper(reader, mock[VerticaFilePartition]).next()
  }

  it should "perform cleanup on close" in {
    val reader = mock[PartitionReader[InternalRow]]
    (reader.close _).expects()
    val partitions = mock[VerticaFilePartition]
    // Todo: cleanup not yet supported.
    new PartitionReaderWrapper(reader, partitions).close()
  }
}
