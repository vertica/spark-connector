package com.vertica.spark.functests.endtoend

import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Test suites for submitting to a remote driver.
 * */
class RemoteTests(readOpts: Map[String, String], writeOpts: Map[String, String], jdbcConfig: JDBCConfig, fileStoreConfig: FileStoreConfig)
  extends EndToEnd(readOpts, writeOpts, jdbcConfig, fileStoreConfig) {

  // The super reference set master url to local, which forces them to run against a local instance.
  // This suite uses a spark session without a master specified, giving control to the submitter.
  override lazy val spark: SparkSession = SparkSession.builder()
    .appName("Vertica Connector Test Prototype")
    .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
    .getOrCreate()

  /**
   * This test checks the case where remote executors have to perform multiple tasks and see if multiple connections are
   * created. Note that if executors have more cores tasks, then they may be able run all tasks in one go and not trigger
   * the needed interactions.
   * */
  it should "only create 2 jdbc sessions when write" in {
    val rowCount = 100
    val data = (1 to rowCount).map(i => Row(i)).toList
    val schema = new StructType(Array(StructField("col1", IntegerType)))

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
      val sessionCount = rs.getLong(1)
      // We expect only 2 new jdbc connections made
      assert(sessionCount == initialJdbcSessionCount + 2)
    } catch {
      case exception: Exception => fail("Unexpected exception", exception)
    } finally {
      stmt.close()
    }
  }

}
