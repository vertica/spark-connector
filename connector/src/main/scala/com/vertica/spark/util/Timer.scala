package com.vertica.spark.util

import com.typesafe.scalalogging.Logger
import com.vertica.spark.config.LogProvider
import com.vertica.spark.datasource.core.VerticaDistributedFilesystemReadPipe

/**
 * Class for reporting how long operations take
 *
 * @param enabled Timer is enabled. If false, timing will not happen and nothing is logged
 * @param logger Logger for logging how long operation took
 * @param operationName Name of operation being timed
 */
class Timer (val enabled: Boolean, val logger: Logger, val operationName: String ) {

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
