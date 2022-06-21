package com.vertica.spark.util.schema

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.query.VerticaTableTests
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class ComplexTypeSchemaSupportTest extends AnyFlatSpec with MockFactory{

  behavior of "ComplexTypeSchemaSupportTest"

  it should "correctly handles array[binary] type" in {}

}
