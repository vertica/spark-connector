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

import com.typesafe.config.{Config, ConfigFactory}
import example.PrintUtils._
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()

    // Define a Spark master here
    val spark = SparkSession.builder()
      .appName("Vertica-Spark Connector Scala Example")
      .getOrCreate()

    val examples = new Examples(conf, spark)

    val m: Map[String, () => Unit] = Map(
      "columnPushdown" -> examples.columnPushdown,
      "filterPushdown" -> examples.filterPushdown,
      "writeCustomStatement" -> examples.writeCustomStatement,
      "writeCustomCopyList" -> examples.writeCustomCopyList,
      "writeThenReadWithHDFS" -> examples.writeThenReadHDFS,
      "complexArrayExample" -> examples.writeThenReadComplexArray,
      "rowExample" -> examples.writeThenReadRow,
      "mapExample" -> examples.writeMap,
      "createExternalTable" -> examples.createExternalTable,
      "writeDataUsingMergeKey" -> examples.writeDataUsingMergeKey,
      "writeThenReadWithS3" -> examples.writeThenReadWithS3,
      "writeThenReadWithGCS" -> examples.writeThenReadWithGCS,
      "writeThenReadWithKerberos" -> examples.writeThenReadWithKerberos
    )

    def printAllExamples(): Unit = {
      println("Examples available: ")
      m.keySet.foreach(exampleName => println(s"- $exampleName"))
    }

    def noCase(): Unit = {
      println("No example with that name.")
      printAllExamples()
    }

    if (args.length != 1) {
      println("No example specified!")
      println("Usage: <example-name>")
      printAllExamples()
    }
    else {
      val f: () => Unit = m.getOrElse(args.head, noCase)
      try {
        f()
      }
      catch {
        case e: Exception => {
          e.printStackTrace()
          printFailed("Unexpected error.")
        }
      }
    }
    spark.close()
  }
}
