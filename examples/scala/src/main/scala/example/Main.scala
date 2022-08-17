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

import example.PrintUtils._
import example.examples.{BasicReadWriteExamples, ComplexTypeExamples, ConnectorOptionsExamples}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // Define a Spark master here
    val spark = SparkSession.builder()
      .appName("Vertica-Spark Connector Scala Example")
      .getOrCreate()

    val basicExamples = new BasicReadWriteExamples(spark)
    val ctExamples = new ComplexTypeExamples(spark)
    val optExamples = new ConnectorOptionsExamples(spark)

    val m: Map[String, () => Unit] = Map(
      "writeCustomStatement" -> optExamples.writeCustomStatement,
      "writeCustomCopyList" -> optExamples.writeCustomCopyList,
      "writeThenRead" -> basicExamples.writeThenRead,
      "complexArrayExample" -> ctExamples.writeThenReadComplexArray,
      "writeThenReadRow" -> ctExamples.writeThenReadRow,
      "writeMap" -> ctExamples.writeMap,
      "writeThenReadExternalTable" -> basicExamples.writeThenReadExternalTable,
      "writeDataUsingMergeKey" -> optExamples.writeDataUsingMergeKey,
      "writeThenReadWithS3" -> basicExamples.writeThenReadWithS3,
      "writeThenReadWithGCS" -> basicExamples.writeThenReadWithGCS,
      "writeThenReadWithKerberos" -> basicExamples.writeThenReadWithKerberos
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
