package com.vertica.spark.datasource.wrappers

import com.vertica.spark.common.TestObjects
import com.vertica.spark.datasource.partitions.file.VerticaFilePartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class VerticaScanWrapperTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  behavior of "VerticaScanWrapperTest"

  private val readConfig = TestObjects.readConfig

  it should "convert itself to Batch" in {
    val instance = new VerticaScanWrapper(mock[Scan], readConfig)
    assert(instance.toBatch == instance)
  }

  it should "correctly record partitioning information" in {
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

    val partitions = new VerticaScanWrapper(scan, readConfig).planInputPartitions()
    assert(partitions.head.isInstanceOf[VerticaFilePartition])

    {
      // Checking first partition
      val verticaFilePartition = partitions.head.asInstanceOf[VerticaFilePartition]
      assert(verticaFilePartition.index == 1)

      // Checking each VerticaFilePartition has the correct identities
      assert(verticaFilePartition.getPortions.length == verticaFilePartition.files.length)
      assert(verticaFilePartition.getPortions(0).filename == "path1")
      assert(verticaFilePartition.getPortions(1).filename == "path2")

      // Checking partitioning record count
      assert(verticaFilePartition.partitioningRecords.keySet.toList.length == 4)
      assert(verticaFilePartition.partitioningRecords("path1") == 2)
      assert(verticaFilePartition.partitioningRecords("path2") == 2)
      assert(verticaFilePartition.partitioningRecords("path3") == 2)
      assert(verticaFilePartition.partitioningRecords("path4") == 1)
    }

    {
      // Checking second partition
      val verticaFilePartition = partitions(1).asInstanceOf[VerticaFilePartition]
      assert(verticaFilePartition.index == 2)

      // Checking each VerticaFilePartition has the correct identities
      assert(verticaFilePartition.getPortions.length == verticaFilePartition.files.length)
      assert(verticaFilePartition.getPortions(0).filename == "path2")
      assert(verticaFilePartition.getPortions(1).filename == "path1")
      assert(verticaFilePartition.getPortions(2).filename == "path3")

      // Checking partitioning record count
      assert(verticaFilePartition.partitioningRecords.keySet.toList.length == 4)
      assert(verticaFilePartition.partitioningRecords("path1") == 2)
      assert(verticaFilePartition.partitioningRecords("path2") == 2)
      assert(verticaFilePartition.partitioningRecords("path3") == 2)
      assert(verticaFilePartition.partitioningRecords("path4") == 1)
    }

    {
      // Checking third partition
      val verticaFilePartition = partitions(2).asInstanceOf[VerticaFilePartition]
      assert(verticaFilePartition.index == 3)

      // Checking each VerticaFilePartition has the correct identities
      assert(verticaFilePartition.getPortions.length == verticaFilePartition.files.length)
      assert(verticaFilePartition.getPortions(0).filename == "path3")
      assert(verticaFilePartition.getPortions(1).filename == "path4")

      // Checking partitioning record count
      assert(verticaFilePartition.partitioningRecords.keySet.toList.length == 4)
      assert(verticaFilePartition.partitioningRecords("path1") == 2)
      assert(verticaFilePartition.partitioningRecords("path2") == 2)
      assert(verticaFilePartition.partitioningRecords("path3") == 2)
      assert(verticaFilePartition.partitioningRecords("path4") == 1)
    }
  }

  it should "create PartitionReaderWrapperFactory" in {
    val scan = mock[Scan]
    val batch = mock[Batch]
    (scan.toBatch _).expects().returning(batch)
    (batch.createReaderFactory _).expects().returning(mock[PartitionReaderFactory])

    new VerticaScanWrapper(scan, readConfig).createReaderFactory().isInstanceOf[PartitionReaderWrapperFactory]
  }

  it should "read schema of the wrapped Scan" in {
    val scan = mock[Scan]
    (scan.readSchema _).expects().returning(StructType(Nil))

    new VerticaScanWrapper(scan, readConfig).readSchema()
  }

}
