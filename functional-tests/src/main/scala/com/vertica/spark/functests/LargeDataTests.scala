package com.vertica.spark.functests

import java.sql.Connection
import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import com.vertica.spark.functests.endtoend.EndToEnd
import com.vertica.spark.util.error.ConnectorException
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class LargeDataTests(readOpts: Map[String, String], writeOpts: Map[String, String], jdbcConfig: JDBCConfig, fileStoreConfig: FileStoreConfig)
  extends EndToEnd(readOpts, writeOpts, jdbcConfig, fileStoreConfig){

  val numSparkPartitions = 4

  it should "save a 1600 column table using default copy logic." in {
    val tableName = "1600ColumnTable"

    val options = writeOpts + ("table" -> tableName)
    val df = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true").load("src/main/resources/1600ColumnTable.csv")

    val numDfRows = df.count()
    val stmt = conn.createStatement()
    stmt.execute("DROP TABLE IF EXISTS " + "\"" + options("table") + "\";")

    val mode = SaveMode.Append

    try {
      df.write.format("com.vertica.spark.datasource.VerticaSource").options(options).mode(mode).save()
    } catch {
      case e: ConnectorException => fail(e.error.getFullContext)
    }

    var totalRows = 0
    val query = "SELECT COUNT(*) AS count FROM " + "\"" + options("table") + "\";"
    try {
      val rs = stmt.executeQuery(query)
      if (rs.next) {
        totalRows = rs.getInt("count")
      }
    }
    finally {
      stmt.close()
    }
    assert (totalRows == numDfRows)
    TestUtils.dropTable(conn, tableName)
  }
}
