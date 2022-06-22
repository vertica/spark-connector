package com.vertica.spark.datasource.partitions.parquet

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class ParquetFileRangeTest extends AnyFlatSpec with MockFactory{

  behavior of "ParquetFileRangeTest"

  val fileRange = ParquetFileRange("filename", 10, 20, 30)

  it should "return correct filename" in {
    assert(fileRange.filename == "filename")
  }

  it should "return correct index" in {
    assert(fileRange.index == 30)
  }

}
