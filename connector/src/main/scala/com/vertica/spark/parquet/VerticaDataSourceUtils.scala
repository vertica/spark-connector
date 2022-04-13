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
package org.apache.spark.sql.execution.datasources.parquet.vertica

import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy

/**
 * Copied from Spark 3.2.0 DataSourceUtils implementation.
 *
 * Classes under the parquet package were copied from Spark for reading parquet into Spark. It is not a public API and
 * thus is expected to change. In Spark 3.2.1, the DataSourceUtils interface was changed. As a fix, this class
 * copied from Spark 3.2.0 only the needed functions.
 * */
object VerticaDataSourceUtils {

  def createDateRebaseFuncInRead(
                                 rebaseMode: LegacyBehaviorPolicy.Value,
                                 format: String): Int => Int = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => days: Int =>
      if (days < RebaseDateTime.lastSwitchJulianDay) {
        throw DataSourceUtils.newRebaseExceptionInRead(format)
      }
      days
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseJulianToGregorianDays
    case LegacyBehaviorPolicy.CORRECTED => identity[Int]
  }

  def createTimestampRebaseFuncInRead(
                                      rebaseMode: LegacyBehaviorPolicy.Value,
                                      format: String): Long => Long = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => micros: Long =>
      if (micros < RebaseDateTime.lastSwitchJulianTs) {
        throw DataSourceUtils.newRebaseExceptionInRead(format)
      }
      micros
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseJulianToGregorianMicros
    case LegacyBehaviorPolicy.CORRECTED => identity[Long]
  }
}
