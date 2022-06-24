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

package com.vertica.spark.datasource.json

import com.vertica.spark.config.{DistributedFilesystemReadConfig, LogProvider, ReadConfig}
import com.vertica.spark.datasource.core.DSConfigSetupInterface
import com.vertica.spark.datasource.v2.VerticaScan
import com.vertica.spark.util.error.{ErrorHandling, InitialSetupPartitioningError}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

/**
 * We support reading JSON files by re-using Spark's JSON support implemented in [[JsonTable]].
 * */
class VerticaJsonScan(config: ReadConfig, readConfigSetup: DSConfigSetupInterface[ReadConfig], batchFactory: JsonBatchFactory) extends Scan with Batch {

  private val logger = LogProvider.getLogger(classOf[VerticaScan])

  private val jsonReadConfig = config match {
    case cfg: DistributedFilesystemReadConfig =>
      val copied = cfg.copy(useJson = true)
      copied.setGroupBy(cfg.getGroupBy)
      copied.setPushdownAgg(cfg.isAggPushedDown)
      copied.setPushdownFilters(cfg.getPushdownFilters)
      copied.setRequiredSchema(cfg.getRequiredSchema)
      copied
    case _ => config
  }

  private lazy val batch: Batch = {
    // Export JSON before initializing Spark's JSON support.
    readConfigSetup.performInitialSetup(jsonReadConfig) match {
      case Left(err) => ErrorHandling.logAndThrowError(logger, err)
      case Right(opt) => opt match {
        case None => ErrorHandling.logAndThrowError(logger, InitialSetupPartitioningError())
        case Some(partitionInfo) =>
          val sparkSession = SparkSession.getActiveSession.getOrElse(ErrorHandling.logAndThrowError(logger, InitialSetupPartitioningError()))
          batchFactory.build(partitionInfo.rootPath, Some(readSchema()), jsonReadConfig, sparkSession)
      }
    }
  }

  //Todo: need to infer complex type schema from Vertica tables.
  override def readSchema(): StructType = {
    (readConfigSetup.getTableSchema(config), jsonReadConfig.getRequiredSchema) match {
      case (Right(schema), requiredSchema) => if (requiredSchema.nonEmpty) { requiredSchema } else { schema }
      case (Left(err), _) => ErrorHandling.logAndThrowError(logger, err)
    }
  }

  override def planInputPartitions(): Array[InputPartition] = batch.planInputPartitions()

  override def createReaderFactory(): PartitionReaderFactory = batch.createReaderFactory()

  override def toBatch: Batch = this

}
