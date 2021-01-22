package com.vertica.spark.functests

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class EndToEndTests extends AnyFlatSpec {

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  it should "read data from Vertica" in {
    val readOpts = Map(
      "host" -> "eng-g9-051",
      "user" -> "release",
      "db" -> "testdb",
      "staging_fs_url" -> "hdfs://eng-g9-051:8020/data/testhdfs.parquet",
      "password" -> "",
      "tablename" -> "footable"
    )

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts).load()

    df.rdd.foreach(row => assert(row.getAs[Int](0) == 2))
  }
}
