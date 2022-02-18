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

package com.vertica.spark.util.listeners

import com.vertica.spark.config.{DistributedFilesystemReadConfig, LogProvider}
import com.vertica.spark.datasource.fs.HadoopFileStoreLayer
import com.vertica.spark.util.error.ConnectorError
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession

object ApplicationParquetCleaner {
  def register(config: DistributedFilesystemReadConfig): ConnectorResult[Unit] = {
    SparkSession.getActiveSession match {
      case Some(session) =>
        val cleaner = new ApplicationParquetCleaner(config)
        session.sparkContext.addSparkListener(cleaner)
        Right()
      case None => Left(CleanerRegistrationError())
    }
  }
}

/**
 * This listener is called at the end of Spark app to remove the export folder.
 * */
class ApplicationParquetCleaner(config: DistributedFilesystemReadConfig) extends SparkListener {
  private val logger = LogProvider.getLogger(classOf[ApplicationParquetCleaner])

  private val fileStoreLayer = new HadoopFileStoreLayer(config.fileStoreConfig, config.metadata match {
    case Some(metadata) => if (config.getRequiredSchema.nonEmpty) {
      Some(config.getRequiredSchema)
    } else {
      Some(metadata.schema)
    }
    case _ => None
  })

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val fileStoreConfig = config.fileStoreConfig
    if (!config.fileStoreConfig.preventCleanup) {
      val hdfsPath = fileStoreConfig.address
      fileStoreLayer.removeDir(hdfsPath) match {
        case Right(_) => logger.info("Removed " + hdfsPath)
        case Left(error) => logger.info(error.toString)
      }
    }
  }
}

case class CleanerRegistrationError() extends ConnectorError {
  def getFullContext: String = "Failed to add application shutdown listener to context"
}
