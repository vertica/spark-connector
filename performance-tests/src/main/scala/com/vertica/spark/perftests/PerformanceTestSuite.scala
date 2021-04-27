package com.vertica.spark.perftests

import org.apache.orc.impl.TreeReaderFactory.StructTreeReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

sealed trait TestMode
case class WriteMode() extends TestMode
case class ReadMode() extends TestMode
case class BothMode() extends TestMode

sealed trait TestDataSourceType
case class V2Source() extends TestDataSourceType
case class V1Source() extends TestDataSourceType
case class JdbcSparkSource() extends TestDataSourceType

case class DataRunDef(opts: Map[String, String],
                      df: DataFrame,
                      cols: Int,
                      rows: Int,
                      runs: Int,
                      mode: TestMode,
                      sourceType: TestDataSourceType,
                      filter: String)

class PerformanceTestSuite(spark: SparkSession) {
  def discardOutliersAndAverageRuns(dataRunDef: DataRunDef): Unit = {
    dataRunDef.sourceType match {
      case _ : JdbcSparkSource => println("TESTING W/ JDBC")
      case _ : V2Source => println("TESTING W/ V2 Connector")
      case _ : V1Source => println("TESTING W/ V1 Connector")
    }
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
    if(dataRunDef.sourceType.isInstanceOf[JdbcSparkSource]) jdbcTestWrite(dataRunDef) else colTestWrite(dataRunDef)
    val endTime: Long = System.currentTimeMillis()
    println("Write run for col200row12M -- run " + runNum + " start: " + startTime + ", end: " + endTime)
    endTime - startTime
  }

  def timeRead(dataRunDef: DataRunDef, runNum: Int) = {
    val startTime: Long = System.currentTimeMillis()
    if(dataRunDef.sourceType.isInstanceOf[JdbcSparkSource]) jdbcTestRead(dataRunDef) else colTestRead(dataRunDef)
    val endTime: Long = System.currentTimeMillis()
    println("Read run for col200row12M -- run " + runNum + " start: " + startTime + ", end: " + endTime)
    endTime - startTime
  }

  def tableName(dataRunDef: DataRunDef) = "t" + dataRunDef.cols + "col" + dataRunDef.rows / 1000000 + "Mrow"

  def colTestWrite(dataRunDef: DataRunDef): Unit = {
    val tablename = tableName(dataRunDef)
    val mode = SaveMode.Overwrite
    val sourceString = if(dataRunDef.sourceType.isInstanceOf[V1Source]) "com.vertica.spark.datasource.DefaultSource" else "com.vertica.spark.datasource.VerticaSource"
    dataRunDef.df.write.format(sourceString).options(dataRunDef.opts + ("table" -> tablename)).mode(mode).save()
  }

  def colTestRead(dataRunDef: DataRunDef): Unit = {
    val tablename = tableName(dataRunDef)
    val sourceString = if(dataRunDef.sourceType.isInstanceOf[V1Source]) "com.vertica.spark.datasource.DefaultSource" else "com.vertica.spark.datasource.VerticaSource"
    var dfRead: DataFrame = spark.read.format(sourceString).options(dataRunDef.opts + ("table" -> tablename)).load()
    if(dataRunDef.filter.nonEmpty) dfRead = dfRead.filter(dataRunDef.filter)
    val count = dfRead.rdd.count()
    println("READ COUNT: " + count + ", EXPECTED " + dataRunDef.rows)
  }

  def jdbcTestRead(dataRunDef: DataRunDef): Unit = {
    val tablename = tableName(dataRunDef)
    var jdbcDf = spark.read.format("jdbc")
      .option("url", "jdbc:vertica://" + dataRunDef.opts("host") + ":5433" + "/" + dataRunDef.opts("db") + "?user="+
        dataRunDef.opts("user")+"&password="+dataRunDef.opts("password"))
      .option("dbtable", tablename)
      .option("driver", "com.vertica.jdbc.Driver")
      .option("partitionColumn", "col1")
      .option("lowerBound", Int.MinValue)
      .option("upperBound", Int.MaxValue)
      .option("numPartitions", 16)
      .load()
    if(dataRunDef.filter.nonEmpty) jdbcDf = jdbcDf.filter(dataRunDef.filter)
    val count = jdbcDf.rdd.count()
    println("JDBC READ COUNT: " + count + ", EXPECTED " + dataRunDef.rows)
  }

  def jdbcTestWrite(dataRunDef: DataRunDef): Unit = {
    val tablename = tableName(dataRunDef)
    val createTableColumns = DataGenUtils.getColumns(dataRunDef.cols)
    //val createTableStatement = "CREATE TABLE " + tablename + "(" + createTableColumns + ")"
    //println("create table statement: " + createTableStatement)
    val mode = SaveMode.Overwrite
    dataRunDef.df.write.format("jdbc")
      .option("url", "jdbc:vertica://" + dataRunDef.opts("host") + ":5433" + "/" + dataRunDef.opts("db") + "?user="+
        dataRunDef.opts("user")+"&password="+dataRunDef.opts("password"))
      .option("dbtable", tablename)
      .option("driver", "com.vertica.jdbc.Driver")
      .option("numPartitions", 16)
      .option("createTableColumnTypes", createTableColumns)
      .mode(mode)
      .save()
  }

  def runAndTimeTests(optsList: Array[Map[String, String]], colCounts: String, rowCounts: String, runCount: Int, testMode: TestMode, testAgainstJdbc: Boolean, testAgainstV1: Boolean, numPartitions: Int, filter: String): Unit = {

    optsList.map(opts => {
      println("Running operation with options: " + opts.toString())
      val dataGenUtils = new DataGenUtils(opts("staging_fs_url"), spark)

      colCounts.split(",").map(x => x.toInt).map(colCount => {
        rowCounts.split(",").map(x => x.toInt).map(rowCount => {
          val rowsPerPartition = rowCount / numPartitions
          val df = dataGenUtils.loadOrGenerateData(rowsPerPartition, numPartitions, colCount)

          val runDef = DataRunDef(opts, df, colCount, rowsPerPartition * numPartitions, runCount, testMode, V2Source(), filter)
          discardOutliersAndAverageRuns(runDef)
          if(testAgainstJdbc) {
            val jdbcRunDef = DataRunDef(opts, df, colCount, rowsPerPartition * numPartitions, runCount, testMode, JdbcSparkSource(), filter)
            discardOutliersAndAverageRuns(jdbcRunDef)
          }
          if(testAgainstV1) {
            val jdbcRunDef = DataRunDef(opts, df, colCount, rowsPerPartition * numPartitions, runCount, testMode, V1Source(), filter)
            discardOutliersAndAverageRuns(jdbcRunDef)
          }
        })
      })
    })
  }

}
