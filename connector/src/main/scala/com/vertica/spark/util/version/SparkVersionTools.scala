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

package com.vertica.spark.util.version

import com.vertica.spark.config.ReadConfig
import com.vertica.spark.datasource.core.DSConfigSetupInterface
import com.vertica.spark.datasource.v2.VerticaScanBuilder
import com.vertica.spark.util.reflections.ReflectionTools
import com.vertica.spark.util.version.SparkVersionTools.{SPARK_3_2_0, SPARK_3_3_0}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.Expression

import scala.util.Try

class SparkVersionTools(reflection: ReflectionTools = new ReflectionTools) {

  /**
   * @return the version string of Spark
   * */
  def getVersionString: Option[String] = SparkSession.getActiveSession.map(_.version)

  /**
   * @return a [[Version]] from a Spark version string
   * */
  def getVersion: Option[Version] = getVersion(getVersionString)

  /**
   * @return a [[Version]] from a Spark version string
   * */
  def getVersion(versionStr: Option[String]): Option[Version] = versionStr match {
    case Some(str) =>
      val regex = "([0-9]+)\\.([0-9]+)\\.([0-9]+)(.*)".r
      Try {
        val regex(major, minor, service, _) = str
        Some(Version(major.toInt, minor.toInt, service.toInt))
      }.getOrElse(None)
    case None => None
  }

  /**
   * @return a compatible [[VerticaScanBuilder]] for the given spark version.
   * */
  def makeCompatibleVerticaScanBuilder(sparkVersion: Version, config: ReadConfig, readSetupInterface: DSConfigSetupInterface[ReadConfig]): VerticaScanBuilder = {
    val sparkSupportsAggregatePushDown = sparkVersion.largerOrEqual(SPARK_3_2_0)
    if (sparkSupportsAggregatePushDown) {
      reflection.makeScanBuilderWithPushDown(config, readSetupInterface)
    } else {
      reflection.makeScanBuilderWithoutPushDown(config, readSetupInterface)
    }
  }

  /**
   * Since the connector compiles against the latest version of Spark, for backward compatibility this function uses
   * reflection to invoke the appropriate method that returns group-by expressions.
   *
   * @return an array of [[Expression]] reprsenting the group-by columns.
   * */
  def getCompatibleGroupByExpressions(sparkVersion: Version, aggObj: Aggregation): Array[Expression] = {
    if(sparkVersion.lessThan(SPARK_3_3_0)){
      // $COVERAGE-OFF$
      reflection.aggregationInvokeMethod[Array[Expression]](aggObj, "groupByColumns")
      // $COVERAGE-ON$
    } else {
      aggObj.groupByExpressions()
    }
  }
}

object SparkVersionTools {
  val SPARK_3_3_0 = Version(3, 3)
  val SPARK_3_2_0 = Version(3, 2)
}
