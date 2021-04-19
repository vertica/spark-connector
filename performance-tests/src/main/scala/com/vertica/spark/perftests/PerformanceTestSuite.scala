package com.vertica.spark.perftests

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class DataRunDef(opts: Map[String, String], df: DataFrame, cols: Int, rows: Int, runs: Int) {
}

class PerformanceTestSuite(spark: SparkSession) {
  def timeRun(dataRunDef: DataRunDef): Long = {
    discardOutliersAndAverageRuns(dataRunDef)
  }

  def discardOutliersAndAverageRuns(dataRunDef: DataRunDef): Long = {
    val results = (0 until dataRunDef.runs).map( i => {
      val tablename = dataRunDef.cols + "col" + dataRunDef.rows / 1000000 + "Mrow"
      val startTime: Long = System.currentTimeMillis()
      colTest(dataRunDef.opts, dataRunDef.df, tablename)
      val endTime: Long = System.currentTimeMillis()
      println("Run for col200row12M -- run " + i + " start: " + startTime + ", end: " + endTime)
      endTime - startTime
    })

    val culledResults = if(dataRunDef.runs >= 5) {
      results.filter(v => v != results.min && v != results.max)
    } else results

    culledResults.sum / culledResults.length
  }

  def colTest(opts: Map[String, String], df: DataFrame, tableName: String): Unit = {
    val mode = SaveMode.Overwrite

    df.write.format("com.vertica.spark.datasource.VerticaSource").options(opts + ("table" -> tableName)).mode(mode).save()

    val dfRead: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(opts + ("table" -> tableName)).load()

    dfRead.rdd.count()
  }

  def runAndTimeTests(opts: Map[String, String], colCounts: String, rowCounts: String, runCount: Int): Unit = {
    val dataGenUtils = new DataGenUtils(opts("staging_fs_url"), spark)

    colCounts.split(",").map(x => x.toInt).map(colCount => {
      rowCounts.split(",").map(x => x.toInt).map(rowCount => {
        val rowsPerPartition = rowCount / 25
        val numPartitions = 25
        val df = dataGenUtils.loadOrGenerateData(rowsPerPartition, numPartitions, colCount)

        val runDef = DataRunDef(opts, df, colCount, rowsPerPartition * numPartitions, runCount)
        println("RUNNING PERF TEST FOR ROW COUNT : " + runDef.rows + " , COL COUNT: " + runDef.cols + " -- DOING " + runDef.runs + " RUNS")
        val time = timeRun(runDef)

        println("RAN PERF TEST, TOOK AVERAGE OF: " + time + " MS")

        time
      })
    })
  }

}
