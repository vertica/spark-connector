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

package com.vertica.spark.util

import com.typesafe.scalalogging.Logger

/**
 * Class for reporting how long operations take
 *
 * @param enabled Timer is enabled. If false, timing will not happen and nothing is logged
 * @param logger Logger for logging how long operation took
 * @param operationName Name of operation being timed
 */
class Timer (val enabled: Boolean, val logger: Logger, val operationName: Sring ) {

  var t0 = 0L

  def startTime(): Unit = {
    if(enabled) {
      t0 = System.currentTimeMillis();
    }
  }

  def endTime(): Unit = {
    if(enabled) {
      val t1 = System.currentTimeMillis();
      logger.info("Timed operation: " + operationName + " -- took " + (t1-t0) + " ms.");
    }
  }
}
