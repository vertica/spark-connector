package com.vertica.spark.datasource.wrappers

import com.vertica.spark.common.TestObjects
import com.vertica.spark.datasource.partitions.mixin.Cleanup
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class PartitionReaderWrapperFactoryTest extends AnyFlatSpec with MockFactory{

  behavior of "PartitionReaderWrapperFactoryTest"

  trait MockInputPartition extends InputPartition with Cleanup

  it should "create a PartitionReaderWrapper" in {
    val readerFactory = mock[PartitionReaderFactory]
    val inputPartition = mock[MockInputPartition]
    val partitionReader = mock[PartitionReader[InternalRow]]
    (readerFactory.createReader _).expects(inputPartition).returning(partitionReader)

    val reader = new PartitionReaderWrapperFactory(readerFactory, TestObjects.readConfig).createReader(inputPartition)
    assert(reader.isInstanceOf[PartitionReaderWrapper])
  }

}
