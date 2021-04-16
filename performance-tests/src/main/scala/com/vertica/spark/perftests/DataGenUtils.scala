package com.vertica.spark.perftests

import com.vertica.spark.perftests.DataGenUtils.genDataSchema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, Decimal, DecimalType, IntegerType, StringType, StructField, StructType}

object DataGenUtils  {
  val rand = new scala.util.Random(System.currentTimeMillis)
  def randomStringGen(length: Int): String = rand.alphanumeric.take(length).mkString

  def randomIntGen(): Int = rand.nextInt()

  def randomDecimalGen(): Decimal = Decimal(rand.nextDouble())

  def randomDateGen(): java.sql.Date = {
    val ms = -946771200000L + (Math.abs(rand.nextLong) % (70L * 365 * 24 * 60 * 60 * 1000))
    new java.sql.Date(ms)
  }

  private def columnType(i: Int) = {
    i % 4 match {
      case 0 => StringType
      case 1 => IntegerType
      case 2 => DecimalType(25,10)
      case 3 => DateType
    }
  }

  def genDataRow(colCount: Int): Row = {
    val data = (0 until colCount).map(i => columnType(i) match {
      case StringType => randomStringGen(10)
      case IntegerType => randomIntGen()
      case DecimalType() => randomDecimalGen()
      case DateType => randomDateGen()
    })
    Row.fromSeq(data)
  }

  def genDataSchema(colCount: Int): StructType = {
    StructType(
      (0 until colCount).map(i => StructField("col"+i, columnType(i)))
    )
  }
}

class DataGenUtils(hdfsPath: String, spark: SparkSession) {

  def loadOrGenerateData(rowsPerPartition: Int, numPartitions: Int, colCount: Int): DataFrame = {
    val totalRowCount = rowsPerPartition * numPartitions
    println("Getting data for row count " + totalRowCount + " , col count " + colCount)
    val dataFileName = hdfsPath + "data_" + rowsPerPartition + "_" + colCount + ".parquet"

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(dataFileName))

    if(exists) {
      println("Data already exists, loading")
      val df = spark.read.parquet(dataFileName)
      df.cache()
      df.rdd.count()
      df
    }
    else {
      println("Data doesn't exist yet, generating")
      val basicData : RDD[Row] = spark.sparkContext.parallelize(Seq[Int](), numPartitions)
        .mapPartitions { _ => {
          (1 to rowsPerPartition).map{_ => Row(1)}.iterator
        }}

      val dataSchema = genDataSchema(colCount)
      println("SCHEMA: " + dataSchema.toString())

      val dataDf = spark.createDataFrame(
        basicData.map(_ => DataGenUtils.genDataRow(colCount)),
        dataSchema
      )

      println("Storing data in file " + dataFileName)
      dataDf.write.parquet(dataFileName)

      dataDf
    }
  }

}
