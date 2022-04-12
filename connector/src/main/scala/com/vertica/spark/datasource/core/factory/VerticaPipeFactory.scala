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

package com.vertica.spark.datasource.core.factory

import com.vertica.spark.config._
import com.vertica.spark.datasource.core._
import com.vertica.spark.datasource.fs.HadoopFileStoreLayer
import com.vertica.spark.datasource.jdbc.VerticaJdbcLayer
import com.vertica.spark.util.cleanup.CleanupUtils
import com.vertica.spark.util.listeners.SparkContextWrapper
import com.vertica.spark.util.schema.SchemaTools
import com.vertica.spark.util.table.TableUtils
import com.vertica.spark.util.version.{SparkVersion, SparkVersionUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * Factory for creating a data pipe to send or retrieve data from Vertica
 *
 * Constructed based on the passed in configuration
 */
trait VerticaPipeFactoryInterface {
  def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface

  def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface

  def closeJdbcLayers(): Unit
}

/**
 * Implementation of the vertica pipe factory
 */
object VerticaPipeFactory extends VerticaPipeFactoryInterface {

  // Maintain a single copy of the read and write JDBC layers
  private var readLayer: Option[VerticaJdbcLayer] = None
  private var writeLayer: Option[VerticaJdbcLayer] = None

  private def checkJdbcLayer(jdbcLayer: Option[VerticaJdbcLayer], jdbcConfig: JDBCConfig): Option[VerticaJdbcLayer] = {
    jdbcLayer match {
      case Some(layer) => if (layer.isClosed()) Some(new VerticaJdbcLayer(jdbcConfig)) else jdbcLayer
      case None => Some(new VerticaJdbcLayer(jdbcConfig))
    }
  }

  private def closeJdbcLayer(jdbcLayer: Option[VerticaJdbcLayer]): Unit = {
    jdbcLayer match {
      case Some(layer) => val _ = layer.close()
      case None =>
    }
  }

  override def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface = {
    config match {
      case cfg: DistributedFilesystemReadConfig =>
        val sparkNewerThan320 = SparkVersionUtils.compareWith32(cfg.sparkVersion.toString) > 0
        val hadoopFileStoreLayer = new HadoopFileStoreLayer(cfg.fileStoreConfig, cfg.metadata match {
          case Some(metadata) => if (cfg.getRequiredSchema.nonEmpty) {
            Some(cfg.getRequiredSchema)
          } else {
            Some(metadata.schema)
          }
          case _ => None
        }, sparkReadNewerThan320 = sparkNewerThan320)
        readLayer = checkJdbcLayer(readLayer, cfg.jdbcConfig)
        val sparkContext: Option[SparkContext] = SparkSession.getActiveSession match {
          case None => None
          case Some(session) => Some(session.sparkContext)
        }
        new VerticaDistributedFilesystemReadPipe(cfg, hadoopFileStoreLayer,
          readLayer.get,
          new SchemaTools,
          new CleanupUtils,
          sparkContext = SparkContextWrapper(sparkContext)
        )
    }
  }

  override def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface = {
    config match {
      case cfg: DistributedFilesystemWriteConfig =>
        val schemaTools = new SchemaTools
        writeLayer = checkJdbcLayer(writeLayer, cfg.jdbcConfig)
        new VerticaDistributedFilesystemWritePipe(cfg,
          new HadoopFileStoreLayer(cfg.fileStoreConfig, Some(cfg.schema), sparkReadNewerThan320 = true),
          writeLayer.get,
          schemaTools,
          new TableUtils(schemaTools, writeLayer.get)
        )
    }
  }

  override def closeJdbcLayers(): Unit = {
    closeJdbcLayer(readLayer)
    closeJdbcLayer(writeLayer)
  }

}


