package com.vertica.spark.functests.endtoend

import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}

/**
 * Test suites for submitting to a remote driver. This suite is meant to be configured with a master node when submitting.
 * */
class RemoteTests(readOpts: Map[String, String], writeOpts: Map[String, String], jdbcConfig: JDBCConfig, fileStoreConfig: FileStoreConfig)
  extends EndToEnd(readOpts, writeOpts, jdbcConfig, fileStoreConfig) {

  // The super reference set master url to local, which forces them to run against a local instance.
  // This suite uses a spark session without a master specified, giving control to the submitter.
  override lazy val spark: SparkSession = SparkSession.builder()
    .appName("Vertica Connector Test Prototype")
    .master("local[4]")
    .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
    .getOrCreate()

  /**
   * This test checks the case where remote executors have to perform multiple tasks and see if multiple connections are
   * created. Note that if executors have more cores tasks, then they may be able run all tasks in one go and not trigger
   * the needed interactions.
   * */
  it should "only create constant number of jdbc sessions when write and read" in {
    val rowCount = 50000
    val data = (1 to rowCount).map(i => Row(i, (0 to 1000).map(i => i).toArray)).toList
    val schema = new StructType(Array(StructField("col1", IntegerType), StructField("col2", ArrayType(IntegerType))))

    val partitionsCount = 100
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).repartition(partitionsCount)
    val query = "select count(client_hostname) from v_monitor.user_sessions where client_type='JDBC Driver';"
    val stmt = conn.createStatement()
    try {
      var rs = stmt.executeQuery(query)
      assert(rs.next)
      val initialJdbcSessionCount = rs.getLong(1)

      val tableName = "dftest"
      df.write.format(VERTICA_SOURCE)
        .options(writeOpts + ("table" -> tableName))
        .mode(SaveMode.Overwrite)
        .save()

      rs = stmt.executeQuery(query)
      assert(rs.next)
      val sessionCountWrite = rs.getLong(1)
      // We expect only 2 new jdbc connections made
      assert(sessionCountWrite == initialJdbcSessionCount + 2)

      spark.read.format("com.vertica.spark.datasource.VerticaSource")
        .options(readOpts +
          ("table" -> "dftest") +
          ("num_partitions"-> "30") +
          ("max_row_group_size_export_mb" -> "1") +
          ("max_file_size_export_mb" -> "1"))
        .load()
        .write.format("json").mode(SaveMode.Overwrite).save("./test-output")

      rs = stmt.executeQuery(query)
      assert(rs.next)
      val sessionCountRead = rs.getLong(1)
      assert(sessionCountRead == initialJdbcSessionCount + 4)

    } catch {
      case exception: Exception => fail("Unexpected exception", exception)
    } finally {
      stmt.close()
    }
    stmt.execute("drop table dftest;")
  }

}
