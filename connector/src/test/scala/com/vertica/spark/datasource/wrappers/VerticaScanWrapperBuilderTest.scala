package com.vertica.spark.datasource.wrappers

import org.apache.spark.sql.connector.read.ScanBuilder
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class VerticaScanWrapperBuilderTest extends AnyFlatSpec with MockFactory {
  it should "build VerticaScanWrapper" in {
    val builder = mock[ScanBuilder]
    (builder.build _).expects().returning(mock[VerticaScanWrapper])
    assert(new VerticaScanWrapperBuilder(builder).build().isInstanceOf[VerticaScanWrapper])
  }
}
