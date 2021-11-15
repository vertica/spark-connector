// (c) Copyright [2020-2021] Micro Focus or one of its affiliates.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package example

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()
    // Notice three new options related to kerberos authentication
    // The option, jaas_config_name, defaults to verticajdbc if not specified
    val opts = Map(
      "host" -> conf.getString("functional-tests.host"),
      "user" -> conf.getString("functional-tests.user"),
      "db" -> conf.getString("functional-tests.db"),
      "staging_fs_url" -> conf.getString("functional-tests.filepath"),
      "kerberos_service_name" -> conf.getString("functional-tests.kerberos_service_name"),
      "kerberos_host_name" -> conf.getString("functional-tests.kerberos_host_name"),
      "jaas_config_name" -> conf.getString("functional-tests.jaas_config_name"))

    val spark = SparkSession.builder()
      .master("spark://172.31.0.5:7077")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    try {
      val tableName = "test"
      val schema = new StructType(Array(StructField("col1", IntegerType)))

      val data = Seq.iterate(0,1000)(_ + 1).map(x => Row(x))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.parquet("hdfs://hdfs:8020/data/test")

      /*val mode = SaveMode.Overwrite

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(opts + ("table" -> tableName)).mode(mode).save()
      println("KERBEROS DEMO, WROTE TABLE")

      val dfRead: DataFrame = spark.read
        .format("com.vertica.spark.datasource.VerticaSource")
        .options(opts + ("table" -> tableName)).load()

      dfRead.rdd.foreach(x => println("KERBEROS DEMO, READ: " + x))*/

    } finally {
      spark.close()
    }
  }
}
