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

import org.apache.spark.sql.SparkSession

object SparkVersionUtils {
  val DEFAULT_SPARK = SparkVersion(3,2,0)
  def getVersion(sparkSession: SparkSession): SparkVersion ={
    try {
      val sparkVersion = sparkSession.version
      val versionList = sparkVersion.split("\\.").map(_.toInt)
      SparkVersion(versionList(0), versionList(1), versionList(2))
    }
    catch {
      // Couldn't recgonize version string, default to 3.2.0
      case _: Throwable => DEFAULT_SPARK
    }
  }
}

case class SparkVersion(major: Int, minor: Int, patch: Int) extends Ordered[SparkVersion] {
  override def toString: String = s"$major.$minor.$patch"
  override def compare(that: SparkVersion): Int =
    (this.major * 100 + this.minor * 10 + this.patch) -
      (that.major * 100 + that.minor * 10 + that.patch)
}
