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

class SparkVersionUtils {

  /**
   * @return a tuple containing Spark version numbers in the format (major, minor, patch)
   * */
  def getSparkVersion: (Int, Int, Int) = SparkSession.getActiveSession match {
    case Some(sparkSession) =>
      val parts = sparkSession.version.split("\\.")
      if (parts.length >= 2) {
        val major = parts(0).toInt
        val minor = parts(1).toInt
        val patch = parts(2).toInt
        (major, minor, patch)
      } else {
        (0, 0, 0)
      }
    case None => (0, 0, 0)
  }
}
