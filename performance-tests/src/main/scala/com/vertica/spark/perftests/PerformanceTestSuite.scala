package com.vertica.spark.perftests

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class PerformanceTestSuite(spark: SparkSession) {
  def timeRun(opts: Map[String, String], df: DataFrame): Long = {
    discardOutliersAndAverageRuns(opts, df, 5)
  }

  def discardOutliersAndAverageRuns(opts: Map[String, String], df: DataFrame, runs: Int): Long = {
    val results = (0 until runs).map( i => {
      val startTime: Long = System.currentTimeMillis()
      colTest(opts, df)
      val endTime: Long = System.currentTimeMillis()
      println("Run for col200row12M -- run " + i + " start: " + startTime + ", end: " + endTime)
      endTime - startTime
    })

    val culledResults = if(runs >= 5) {
      results.filter(v => v != results.min && v != results.max)
    } else results

    culledResults.sum / culledResults.length
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

    val df = dataGenUtils.loadOrGenerateData(500000, 24, 200)

    val time = timeRun(opts, df)

    println("RAN PERF TEST, TOOK AVERAGE OF: " + time + " MS")
  }

}
