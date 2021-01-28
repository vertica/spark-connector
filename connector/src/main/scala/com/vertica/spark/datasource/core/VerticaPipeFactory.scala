package com.vertica.spark.datasource.core

import ch.qos.logback.classic.Level
import com.vertica.spark.config._
import com.vertica.spark.datasource.fs.HadoopFileStoreLayer
import com.vertica.spark.datasource.jdbc.VerticaJdbcLayer
import com.vertica.spark.util.cleanup.CleanupUtils
import com.vertica.spark.util.schema.SchemaTools

/**
 * Factory for creating a data pipe to send or retrieve data from Vertica
 *
 * Constructed based on the passed in configuration
 */
trait VerticaPipeFactoryInterface {
  def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface

  def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface = ???
}

/**
 * Implementation of the vertica pipe factory
 */
object VerticaPipeFactory extends VerticaPipeFactoryInterface{
  override def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface = {
    config match {
      case cfg: DistributedFilesystemReadConfig => new VerticaDistributedFilesystemReadPipe(cfg, new HadoopFileStoreLayer(DistributedFilesystemWriteConfig(Level.DEBUG), cfg), new VerticaJdbcLayer(cfg.jdbcConfig), new SchemaTools(), CleanupUtils)
    }
  }
  override def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface = ???
}

