package example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.sql.Connection

object RowExample {
  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()

    val options = Map(
      "host" -> conf.getString("functional-tests.host"),
      "user" -> conf.getString("functional-tests.user"),
      "db" -> conf.getString("functional-tests.db"),
      "staging_fs_url" -> conf.getString("functional-tests.filepath"),
      "password" -> conf.getString("functional-tests.password")
    )

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    val tableName = "dftest"
    val schema = new StructType(Array(
      StructField("col1", IntegerType),
      // col2's type correspond to ROW type in Vertica
      StructField("col2", StructType(Array(
        StructField("f1", StringType),
      ))),
    ))

    val data = Seq(
      Row(1, Row("10")),
      Row(2, Row("11")),
      Row(3, Row("12"))
    )

    val verticaSource = "com.vertica.spark.datasource.VerticaSource"
    val writeOpts = options + ("table" -> tableName)
    val mode = SaveMode.Overwrite

    // Create a dataframe and write to Vertica
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .write
      .options(writeOpts)
      .format(verticaSource)
      .mode(mode)
      .save()
  }
}
