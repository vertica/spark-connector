package com.vertica.spark.datasource.json

import com.vertica.spark.common.TestObjects
import com.vertica.spark.datasource.wrappers.VerticaScanWrapper
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{File, PrintWriter}

class JsonBatchFactoryTest extends AnyFlatSpec with BeforeAndAfterAll{

  behavior of "JsonBatchFactoryTest"

  val jsonFile = new File("./test.json" )
  val pw = new PrintWriter(jsonFile)
  pw.write("{\"a\":9}")
  pw.close()

  it should "should build a VerticaScanWrapper" in {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test")
      .getOrCreate()

    val batch = new JsonBatchFactory().build("./test.json", None, TestObjects.readConfig, spark)
    assert(batch.isInstanceOf[VerticaScanWrapper])

    spark.close()
  }

  override protected def afterAll(): Unit = {
    jsonFile.delete
  }
}
