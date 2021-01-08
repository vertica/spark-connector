package com.vertica.spark.datasource.core

import com.vertica.spark.config._

trait VerticaPipeFactoryImpl {
  def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface 
}

class VerticaPipeFactoryDefaultImpl extends VerticaPipeFactoryImpl{
  override def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface = {
    config match {
      case cfg: DistributedFilesystemReadConfig => new VerticaDistributedFilesystemReadPipe(cfg)
    }
  }
}

/**
 * Factory that creates the correct pipe given the configuration specified by the user.
 */
object VerticaPipeFactory {
  var impl : VerticaPipeFactoryImpl = new VerticaPipeFactoryDefaultImpl()

  def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface = {
    impl.getReadPipe(config)
  }

  def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface = ???
}
