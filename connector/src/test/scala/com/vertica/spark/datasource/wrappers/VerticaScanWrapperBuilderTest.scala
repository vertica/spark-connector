package com.vertica.spark.datasource.wrappers

import com.vertica.spark.common.TestObjects
import org.apache.spark.sql.connector.read.ScanBuilder
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class VerticaScanWrapperBuilderTest extends AnyFlatSpec with MockFactory {

  private val readConfig = TestObjects.readConfig

  it should "build VerticaScanWrapper" in {
    val builder = mock[ScanBuilder]
    (builder.build _).expects().returning(mock[VerticaScanWrapper])
    assert(new VerticaScanWrapperBuilder(builder, readConfig).build().isInstanceOf[VerticaScanWrapper])
  }
}
