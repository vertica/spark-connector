package com.vertica.spark.datasource.core

import com.vertica.spark.config._

/**
 * Factory that creates the correct pipe given the configuration specified by the user.
 */
object VerticaPipeFactory {
  var readPipeOverride: Option[VerticaPipeInterface with VerticaPipeReadInterface] = None // In place to set a pipe interface to return, useful for testing, usually None

  def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface = {
    readPipeOverride match {
      case None => {
        config match {
          case cfg: DistributedFilesystemReadConfig => new VerticaDistributedFilesystemReadPipe(cfg)
        }
      }
      case Some(pipe) => pipe
    }
  }
  def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface = ???
}
