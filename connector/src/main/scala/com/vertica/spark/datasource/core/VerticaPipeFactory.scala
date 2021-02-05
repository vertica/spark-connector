package com.vertica.spark.datasource.core

import com.vertica.spark.config._
import com.vertica.spark.datasource.fs.HadoopFileStoreLayer
import com.vertica.spark.datasource.jdbc.VerticaJdbcLayer
import com.vertica.spark.util.schema.SchemaTools

/**
 * Factory for creating a data pipe to send or retrieve data from Vertica
 *
 * Constructed based on the passed in configuration
 */
trait VerticaPipeFactoryInterface {
  def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface

  def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface
}

/**
 * Implementation of the vertica pipe factory
 */
object VerticaPipeFactory extends VerticaPipeFactoryInterface{
  override def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface = {
    config match {
      case cfg: DistributedFilesystemReadConfig =>
        val hadoopFileStoreLayer =  new HadoopFileStoreLayer(cfg.logProvider, cfg.metadata match {
          case Some(metadata) => Some(metadata.schema)
          case None => None
        })
        new VerticaDistributedFilesystemReadPipe(cfg, hadoopFileStoreLayer, new VerticaJdbcLayer(cfg.jdbcConfig), new SchemaTools(cfg.logProvider))
    }
  }
  override def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface = {
    config match {
      case cfg: DistributedFilesystemWriteConfig =>
        new VerticaDistributedFilesystemWritePipe(cfg, new HadoopFileStoreLayer(cfg.logProvider, Some(cfg.schema)), new VerticaJdbcLayer(cfg.jdbcConfig), new SchemaTools(logProvider = cfg.logProvider))
    }
  }
}

