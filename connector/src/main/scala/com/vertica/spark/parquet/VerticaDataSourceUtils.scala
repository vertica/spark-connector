package org.apache.spark.sql.execution.datasources.parquet.vertica

import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy

/**
 * Copied from Spark 3.2.0 DataSourceUtils implementation.
 *
 * Classes under the parquet package were copied from Spark for reading parquet into Spark. It is not a public API and
 * thus is expected to change. In Spark 3.2.1, the DataSourceUtils interface was changed. To fix this, this class
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
