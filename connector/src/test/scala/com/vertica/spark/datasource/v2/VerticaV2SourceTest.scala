package com.vertica.spark.datasource.v2

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import com.vertica.spark.datasource._

import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.catalog._
import org.scalamock.scalatest.MockFactory

import java.util

import scala.collection.JavaConversions._

import com.vertica.spark.datasource.core._

trait DummyReadPipe extends VerticaPipeInterface with VerticaPipeReadInterface

class VerticaV2SourceTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory{

  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
  }

  it should "return a Vertica Table" in {
    val source = new VerticaSource()
    val table = source.getTable(new StructType(), Array[Transform](), new util.HashMap[String, String]())

    assert(table.isInstanceOf[VerticaTable])
  }

  it should "return a Write Builder" in {
    val mockLogicalWriteInfo = mock[LogicalWriteInfo]

    val opts = Map("logging_level" -> "ERROR")
    val source = new VerticaSource()
    val table = source.getTable(new StructType(), Array[Transform](), opts )
    val writeTable = table.asInstanceOf[SupportsWrite]
    val writeBuilder = writeTable.newWriteBuilder(mockLogicalWriteInfo)


    assert(writeBuilder.isInstanceOf[VerticaWriteBuilder])
  }


}
