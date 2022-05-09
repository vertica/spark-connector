package example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.sql.Connection

object MapExample {
  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()

    val options = Map(
      "host" -> conf.getString("functional-tests.host"),
      "user" -> conf.getString("functional-tests.user"),
      "db" -> conf.getString("functional-tests.db"),
      "password" -> conf.getString("functional-tests.password")
    )

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    val tableName = "dftest"
    val schema = new StructType(Array(
      StructField("col2", MapType(StringType, IntegerType))
    ))

    val data = Seq(
      Row(Map("key"-> 1))
    )

    val verticaSource = "com.vertica.spark.datasource.VerticaSource"
    val writeOpts = options + (
      "table" -> tableName,
      "create_external_table" -> "true",
      "staging_fs_url" -> (conf.getString("functional-tests.filepath") + "external_data")
    )
    val mode = SaveMode.Overwrite

    // Write to Vertica a table with Map type
    // Note that Map type cannot be queried in Vertica and is only allowed in external tables.
    // Map type currently exists for interoperability with external data.
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .write
      .options(writeOpts)
      .format(verticaSource)
      .mode(mode)
      .save()
  }
}
