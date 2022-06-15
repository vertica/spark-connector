package com.vertica.spark.datasource.wrappers

import com.vertica.spark.json.{VerticaFilePartition, VerticaPartitionedFile}
import com.vertica.spark.util.schema.SchemaToolsTests.mock
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class VerticaScanWrapperTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  behavior of "VerticaScanWrapperTest"

  it should "convert itself to Batch" in {
    val instance = new VerticaScanWrapper(mock[Scan])
    assert(instance.toBatch == instance)
  }

  it should "planInputPartitions" in {
    val inputPartitions = Array(
      FilePartition(1, Array(
        PartitionedFile(InternalRow(), "path1", 0, 1, Array.empty),
        PartitionedFile(InternalRow(), "path2", 0, 1, Array.empty)
      )),
      FilePartition(2, Array(
        PartitionedFile(InternalRow(), "path2", 0, 1, Array.empty),
        PartitionedFile(InternalRow(), "path1", 0, 1, Array.empty),
        PartitionedFile(InternalRow(), "path3", 0, 1, Array.empty)
      )),
      FilePartition(3, Array(
        PartitionedFile(InternalRow(), "path3", 0, 1, Array.empty),
        PartitionedFile(InternalRow(), "path4", 0, 1, Array.empty)
      )),
    )

    val scan = mock[Scan]
    val batch = mock[Batch]
    (scan.toBatch _).expects().returning(batch)
    (batch.planInputPartitions _).expects().returning(inputPartitions.asInstanceOf[Array[InputPartition]])

    val partitions = new VerticaScanWrapper(scan).planInputPartitions()
    assert(partitions.head.isInstanceOf[VerticaFilePartition])

    val verticaFilePartition = partitions.head.asInstanceOf[VerticaFilePartition]
    assert(verticaFilePartition.index == 1)
    assert(verticaFilePartition.files.head.isInstanceOf[VerticaPartitionedFile])

    val fileCounts = verticaFilePartition.partitioningRecords.keySet.toList.length
    assert(fileCounts == 4)
    assert(verticaFilePartition.partitioningRecords("path1") == 2)
    assert(verticaFilePartition.partitioningRecords("path2") == 2)
    assert(verticaFilePartition.partitioningRecords("path3") == 2)
    assert(verticaFilePartition.partitioningRecords("path4") == 1)
  }

  it should "create PartitionReaderWrapperFactory" in {
    val scan = mock[Scan]
    val batch = mock[Batch]
    (scan.toBatch _).expects().returning(batch)
    (batch.createReaderFactory _).expects().returning(mock[PartitionReaderFactory])

    new VerticaScanWrapper(scan).createReaderFactory().isInstanceOf[PartitionReaderWrapperFactory]
  }

  it should "read schema of the wrapped Scan" in {
    val scan = mock[Scan]
    (scan.readSchema _).expects().returning(StructType(Nil))

    new VerticaScanWrapper(scan).readSchema()
  }

}
