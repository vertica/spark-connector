package com.vertica.spark.datasource.core

import com.vertica.spark.config._

/**
 * Factory that creates the correct pipe given the configuration specified by the user.
 */
object VerticaPipeFactory {
  // To be implemented
  def GetReadPipe(config : ReadConfig) : VerticaPipeInterface with VerticaPipeReadInterface = ???
  def GetWritePipe(config : WriteConfig) : VerticaPipeInterface with VerticaPipeWriteInterface = ???
}
