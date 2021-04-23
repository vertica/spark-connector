package com.vertica.spark.perftests

import org.apache.orc.impl.TreeReaderFactory.StructTreeReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

sealed trait TestMode
case class WriteMode() extends TestMode
case class ReadMode() extends TestMode
case class BothMode() extends TestMode

case class DataRunDef(opts: Map[String, String], df: DataFrame, cols: Int, rows: Int, runs: Int, mode: TestMode, jdbc: Boolean)

class PerformanceTestSuite(spark: SparkSession) {
  def discardOutliersAndAverageRuns(dataRunDef: DataRunDef): Unit = {
    val mode = dataRunDef.mode
    if(mode.isInstanceOf[ReadMode]) {
      colTestWrite(dataRunDef)
    }
    else {
      println("RUNNING WRITE PERF TEST FOR ROW COUNT : " + dataRunDef.rows + " , COL COUNT: " + dataRunDef.cols + " -- DOING " + dataRunDef.runs + " RUNS")
      val results = (0 until dataRunDef.runs).map( i => {
          timeWrite(dataRunDef, i)
      })

      val culledResults = if(dataRunDef.runs >= 5) {
        results.filter(v => v != results.min && v != results.max)
      } else results

      val avg = culledResults.sum / culledResults.length
      println("RAN WRITE PERF TEST, TOOK AVERAGE OF: " + avg + " MS")
    }

    if(!mode.isInstanceOf[WriteMode]) {
      if(dataRunDef.jdbc) println("TESTING W/ JDBC")
      println("RUNNING READ PERF TEST FOR ROW COUNT : " + dataRunDef.rows + " , COL COUNT: " + dataRunDef.cols + " -- DOING " + dataRunDef.runs + " RUNS")
      val results = (0 until dataRunDef.runs).map( i => {
        timeRead(dataRunDef, i)
      })

      val culledResults = if(dataRunDef.runs >= 5) {
        results.filter(v => v != results.min && v != results.max)
      } else results

      val avg = culledResults.sum / culledResults.length
      println("RAN READ PERF TEST, TOOK AVERAGE OF: " + avg + " MS")
    }
  }

  def timeWrite(dataRunDef: DataRunDef, runNum: Int) = {
    val startTime: Long = System.currentTimeMillis()
    colTestWrite(dataRunDef)
    val endTime: Long = System.currentTimeMillis()
    println("Write run for col200row12M -- run " + runNum + " start: " + startTime + ", end: " + endTime)
    endTime - startTime
  }

  def timeRead(dataRunDef: DataRunDef, runNum: Int) = {
    val startTime: Long = System.currentTimeMillis()
    if(dataRunDef.jdbc) jdbcTestRead(dataRunDef) else colTestRead(dataRunDef)
    val endTime: Long = System.currentTimeMillis()
    println("Read run for col200row12M -- run " + runNum + " start: " + startTime + ", end: " + endTime)
    endTime - startTime
  }

  def tableName(dataRunDef: DataRunDef) = "t" + dataRunDef.cols + "col" + dataRunDef.rows / 1000000 + "Mrow"

  def colTestWrite(dataRunDef: DataRunDef): Unit = {
    val tablename = tableName(dataRunDef)
    val mode = SaveMode.Overwrite
    dataRunDef.df.write.format("com.vertica.spark.datasource.VerticaSource").options(dataRunDef.opts + ("table" -> tablename)).mode(mode).save()
  }

  def colTestRead(dataRunDef: DataRunDef): Unit = {
    val tablename = tableName(dataRunDef)
    val dfRead: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(dataRunDef.opts + ("table" -> tablename)).load()
    val count = dfRead.rdd.count()
    println("READ COUNT: " + count + ", EXPECTED " + dataRunDef.rows)
  }

  def jdbcTestRead(dataRunDef: DataRunDef): Unit = {
    val tablename = tableName(dataRunDef)
    val jdbcDf = spark.read.format("jdbc")
      .option("url", "jdbc:vertica://" + dataRunDef.opts("host") + ":5433" + "/" + dataRunDef.opts("db") + "?user="+
        dataRunDef.opts("user")+"&password="+dataRunDef.opts("password"))
      .option("dbtable", tablename)
      .option("driver", "com.vertica.jdbc.Driver")
      .load()
    val count = jdbcDf.rdd.count()
    println("JDBC READ COUNT: " + count + ", EXPECTED " + dataRunDef.rows)
  }

  def runAndTimeTests(optsList: Array[Map[String, String]], colCounts: String, rowCounts: String, runCount: Int, testMode: TestMode, testAgainstJdbc: Boolean): Unit = {

    optsList.map(opts => {
      println("Running operation with options: " + opts.toString())
      val dataGenUtils = new DataGenUtils(opts("staging_fs_url"), spark)

      colCounts.split(",").map(x => x.toInt).map(colCount => {
        rowCounts.split(",").map(x => x.toInt).map(rowCount => {
          val rowsPerPartition = rowCount / 25
          val numPartitions = 25
          val df = dataGenUtils.loadOrGenerateData(rowsPerPartition, numPartitions, colCount)

          if(testAgainstJdbc) {
            val jdbcRunDef = DataRunDef(opts, df, colCount, rowsPerPartition * numPartitions, runCount, testMode, true)
            discardOutliersAndAverageRuns(jdbcRunDef)
          }
          val runDef = DataRunDef(opts, df, colCount, rowsPerPartition * numPartitions, runCount, testMode, false)
          discardOutliersAndAverageRuns(runDef)
        })
      })
    })
  }

}
