package com.vertica.spark.functests

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class EndToEndTests(readOpts: Map[String, String], tablename: String, tablename20: String) extends AnyFlatSpec with BeforeAndAfterAll {

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()
  }

  it should "read data from Vertica" in {
    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tablename)).load()

    assert(df.count() == 1)
    df.rdd.foreach(row => assert(row.getAs[Long](0) == 2))
  }

  it should "read 20 rows of data from Vertica" in {
    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tablename20)).load()

    assert(df.count() == 20)
    df.rdd.foreach(row => assert(row.getAs[Long](0) == 2))

  }

  it should "support data frame schema" in {

    //val tableName1 = "dftest1"
    //val stmt = conn.createStatement
    //createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a int, b float)")

    //val insert = "insert into "+ tableName1 + " values(1, 2.2)"
    //populateTableBySQL(stmt, tableName1, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> "dftest1")).load()

    val schema = df.schema
    val sc = StructType(Array(StructField("a",LongType,nullable = true), StructField("b",DoubleType,nullable = true)))

    assert(schema.toString equals sc.toString)
  }
}
