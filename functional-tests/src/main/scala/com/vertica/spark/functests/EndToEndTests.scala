package com.vertica.spark.functests

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class EndToEndTests(readOpts: Map[String, String]) extends AnyFlatSpec with BeforeAndAfterAll {

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()
  }

  it should "read data from Vertica" in {

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts).load()

    assert(df.count() == 1)
    df.rdd.foreach(row => assert(row.getAs[Int](0) == 2))
  }
}
