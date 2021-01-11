package com.vertica.spark.datasource.core

import com.vertica.spark.config._
import com.vertica.spark.connector.fs.DummyFileStoreLayer
import com.vertica.spark.jdbc.VerticaJdbcLayer
import com.vertica.spark.util.schema.SchemaTools

trait VerticaPipeFactoryInterface {
  def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface

  def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface = ???
}

class VerticaPipeFactory extends VerticaPipeFactoryInterface{
  override def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface = {
    config match {
      // TODO: Replace file store layer with real implementation of FileStoreLayerInterface
      case cfg: DistributedFilesystemReadConfig => new VerticaDistributedFilesystemReadPipe(cfg, new DummyFileStoreLayer(), new VerticaJdbcLayer(cfg.jdbcConfig), new SchemaTools())
    }
  }
  override def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface = ???
}

