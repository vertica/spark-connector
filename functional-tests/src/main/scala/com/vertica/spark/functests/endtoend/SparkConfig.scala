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

package com.vertica.spark.functests.endtoend

import org.apache.spark.SparkConf

/**
 * Mixin for creating a base [[SparkConf]] for a spark session.
 * */
trait SparkConfig {

  /**
   * The name that will be displayed on Spark Master UI
   * */
  def sparkAppName: String

  /**
   * Get a base [[SparkConf]]
   *
   * @param remote if false, the config will set master as local[*], else it will be unset.
   * */
  def baseSparkConf(remote: Boolean): SparkConf = {
    val conf = if (remote) {
      new SparkConf()
    }
    else {
      new SparkConf().setMaster("local[*]")
    }
    conf.setAppName(sparkAppName)
  }
}
