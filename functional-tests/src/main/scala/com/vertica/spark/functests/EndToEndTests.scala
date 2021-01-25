package com.vertica.spark.functests

import java.sql.Connection

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class EndToEndTests(readOpts: Map[String, String], tablename: String, tablename20: String) extends AnyFlatSpec with BeforeAndAfterAll {
  val conn: Connection = TestUtils.getJDBCConnection(readOpts("host"), db = readOpts("db"), user = readOpts("user"), password = readOpts("password"))

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()
    conn.close()
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

  it should "support data frame projection" in {
    val tableName1 = "dftest1"
    val stmt = conn.createStatement
    val n = 3
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a int, b float)")
    val insert = "insert into "+ tableName1 + " values(1, 2.2)"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    val filtered = df.select(df("a"))
    val count = filtered.count()

    assert(count == n)
    assert (filtered.columns.mkString equals Array("a").mkString)
  }

  it should "support data frame filter" in {

  }

  it should "load data from Vertica table that is [SEGMENTED] on [ALL] nodes" in {

  }




}
