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
import com.vertica.spark.util.schema.SchemaTools
import com.vertica.spark.util.table.TableUtils

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
object VerticaPipeFactory extends VerticaPipeFactoryInterface {

  // Maintain a single copy of the read and write JDBC layers
  private var readLayer: Option[VerticaJdbcLayer] = None
  private var writeLayer: Option[VerticaJdbcLayer] = None

  override def getReadPipe(config: ReadConfig): VerticaPipeInterface with VerticaPipeReadInterface = {
    config match {
      case cfg: DistributedFilesystemReadConfig =>
        val hadoopFileStoreLayer = new HadoopFileStoreLayer(cfg.fileStoreConfig, cfg.metadata match {
          case Some(metadata) => if (cfg.getRequiredSchema.nonEmpty) {
            Some(cfg.getRequiredSchema)
          } else {
            Some(metadata.schema)
          }
          case _ => None
        })
        val jdbcLayer = readLayer match {
          case Some(layer) => {
            if (layer.isClosed()) {
              readLayer = Some(new VerticaJdbcLayer(cfg.jdbcConfig))
            }
            readLayer.get
          }
          case None => {
            readLayer = Some(new VerticaJdbcLayer(cfg.jdbcConfig))
            readLayer.get
          }
        }
        new VerticaDistributedFilesystemReadPipe(cfg, hadoopFileStoreLayer,
          jdbcLayer,
          new SchemaTools,
          new CleanupUtils
        )
    }
  }

  override def getWritePipe(config: WriteConfig): VerticaPipeInterface with VerticaPipeWriteInterface = {
    config match {
      case cfg: DistributedFilesystemWriteConfig =>
        val schemaTools = new SchemaTools
        val jdbcLayer = writeLayer match {
          case Some(layer) => {
            if (layer.isClosed()) {
              writeLayer = Some(new VerticaJdbcLayer(cfg.jdbcConfig))
            }
            writeLayer.get
          }
          case None => {
            writeLayer = Some(new VerticaJdbcLayer(cfg.jdbcConfig))
            writeLayer.get
          }
        }
        new VerticaDistributedFilesystemWritePipe(cfg,
          new HadoopFileStoreLayer(cfg.fileStoreConfig, Some(cfg.schema)),
          jdbcLayer,
          schemaTools,
          new TableUtils(schemaTools, jdbcLayer)
        )
    }
  }

}
