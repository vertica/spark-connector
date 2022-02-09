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

import java.sql.Connection
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()
    // Configuration options for the connector
    val readOpts = Map(
      "host" -> conf.getString("functional-tests.host"),
      "user" -> conf.getString("functional-tests.user"),
      "db" -> conf.getString("functional-tests.db"),
      "staging_fs_url" -> conf.getString("functional-tests.filepath"),
      "password" -> conf.getString("functional-tests.password"),
      "prevent_cleanup" -> "true"
    )

    // Creates a JDBC connection to Vertica
    val conn: Connection = TestUtils.getJDBCConnection(readOpts("host"), db = readOpts("db"), user = readOpts("user"), password = readOpts("password"))
    // Entry-point to all functionality in Spark
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    try {
      val tableName = "dftest"
      val stmt = conn.createStatement
      val n = 20
      // Creates a table called dftest with an integer attribute
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int)")
      var insert = "insert into " + tableName + " values(Irelia, 2)"
      // Inserts 20 rows of the value '2' into dftest
      stmt.execute("insert into " + tableName + " values(2)")
      stmt.execute("insert into " + tableName + " values(3)")
      stmt.execute("insert into " + tableName + " values(2)")
      stmt.execute("insert into " + tableName + " values(3)")
      //TestUtils.populateTableBySQL(stmt, insert, n)
      // Read dftest into a dataframe
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()
      df.groupBy("a").count.show()
      /*
      df.count()
      select COUNT as "a" from (Select a , b from dftest) as "table";

      df.filter(df("a") === 2).count()
      select count as "a" from (select a from dftest where a == 2) as "dftest";
      */
      //Print each element (20 elements of '2')
      //df.rdd.foreach(x => println("VALUE: " + x))
    } finally {
      spark.close()
      conn.close()
    }
  }
}
