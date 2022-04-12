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
import com.vertica.spark.util.version.SparkVersionUtils
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession

/**
 * This wrapper is created solely for compatibility with unit testing.
 * <br/>
 * Because we could not instantiate during unit test, a dummy class that extends from
 * SparkContext is needed to override the functions we are using. However, SparkContext.addSparkListener's argument
 * use a private interface thus can't be override.
 * */
case class SparkContextWrapper(sparkContext: Option[SparkContext]){

  def addSparkListener(listener:SparkListener): Unit ={
    sparkContext match {
      // We may not get a context if this is executed on executor nodes.
      case None =>
      case Some(context) => context.addSparkListener(listener)
    }
  }
}
/**
 * This listener is called at the end of Spark app to remove the export folder.
 * */
class ApplicationParquetCleaner(config: DistributedFilesystemReadConfig) extends SparkListener {
  private val logger = LogProvider.getLogger(classOf[ApplicationParquetCleaner])
  private val sparkNewerThan320 = SparkVersionUtils.compareWith32(SparkSession.getActiveSession.get.version) > 0
  private val metadata = config.metadata match {
    case Some(metadata) => if (config.getRequiredSchema.nonEmpty) {
      Some(config.getRequiredSchema)
    } else {
      Some(metadata.schema)
    }
    case _ => None
  }
  private val fileStoreLayer = new HadoopFileStoreLayer(config.fileStoreConfig, metadata, sparkNewerThan320)

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val hdfsPath = config.fileStoreConfig.address
    if (!config.fileStoreConfig.preventCleanup) {
      fileStoreLayer.removeDir(hdfsPath) match {
        case Right(_) => logger.info("Removed " + hdfsPath)
        case Left(error) => logger.error(s"Error removing $hdfsPath. ${error.toString}")
      }
    }
  }
}

case class CleanerRegistrationError() extends ConnectorError {
  def getFullContext: String = "Failed to add application shutdown listener to context"
}
