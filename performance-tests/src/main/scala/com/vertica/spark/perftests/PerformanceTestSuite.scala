package com.vertica.spark.perftests

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class PerformanceTestSuite(spark: SparkSession) {
  def timeRun(opts: Map[String, String], df: DataFrame): Long = {
    val startTime: Long = System.currentTimeMillis()
    colTest(opts, df)
    val endTime: Long = System.currentTimeMillis()
    println("start: " + startTime + ", end: " + endTime)
    endTime - startTime
  }

  def colTest(opts: Map[String, String], df: DataFrame): Unit = {
    val tableName = "200coldata"
    val mode = SaveMode.Overwrite

    df.write.format("com.vertica.spark.datasource.VerticaSource").options(opts + ("table" -> tableName)).mode(mode).save()

    val dfRead: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(opts + ("table" -> tableName)).load()

    dfRead.rdd.count()
  }

  def runAndTimeTests(opts: Map[String, String]): Unit = {
    val dataGenUtils = new DataGenUtils(opts("staging_fs_url"), spark)

    val df = dataGenUtils.loadOrGenerateData()

    val time = timeRun(opts, df)

    println("RAN PERF TEST, TOOK: " + time + " MS")
  }

}
