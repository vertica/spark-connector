package com.vertica.spark.datasource.core

import com.vertica.spark.config._
import com.vertica.spark.jdbc.VerticaJdbcLayer
import com.vertica.spark.util.schema.SchemaTools

trait VerticaPipeFactoryImpl {
  def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface
}

class VerticaPipeFactoryDefaultImpl extends VerticaPipeFactoryImpl{
  override def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface = {
    config match {
      case cfg: DistributedFilesystemReadConfig => new VerticaDistributedFilesystemReadPipe(cfg, new VerticaJdbcLayer(cfg.jdbcConfig), new SchemaTools())
    }
  }
}

/**
 * Factory that creates the correct pipe given the configuration specified by the user.
 */
object VerticaPipeFactory {
  var impl: VerticaPipeFactoryImpl = new VerticaPipeFactoryDefaultImpl()

  def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface = {
    impl.getReadPipe(config)
  }

  def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface = ???
}
