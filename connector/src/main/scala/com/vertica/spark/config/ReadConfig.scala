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

package com.vertica.spark.config

import ch.qos.logback.classic.Level
import com.vertica.spark.datasource.v2.PushdownFilter
import org.apache.spark.sql.types.StructType

/**
 * Interface for configuration of a read (from Vertica) operation.
 */
trait ReadConfig extends GenericConfig {

  /**
   * Set filters to push down to the Vertica read.
   * @param pushdownFilters Filter strings
   */
  def setPushdownFilters(pushdownFilters: List[PushdownFilter]): Unit

  /**
   * Sets the required schema of the read operation. Used to push down column selection
   * @param requiredSchema Schema of subset of data to be read from Vertica table
   */
  def setRequiredSchema(requiredSchema: StructType): Unit
  def getRequiredSchema: StructType
}

/**
 * Configuration for a read operation using a distributed filesystem as an intermediary.
 *
 * @param logLevel Logging level for the read operation.
 * @param jdbcConfig Configuration for the JDBC connection used to communicate with Vertica.
 * @param fileStoreConfig Configuration for the intermediary filestore used to stage data between Spark and Vertica.
 * @param tablename Table to read from.
 * @param partitionCount Number of Spark partitions to divide the read into.
 * @param metadata Contains any metadata from the Vertica table that needs to be retrieved before the read. Includes table schema.
 */
final case class DistributedFilesystemReadConfig(
                                                  override val logLevel: Level,
                                                  jdbcConfig: JDBCConfig,
                                                  fileStoreConfig: FileStoreConfig,
                                                  tablename: TableName,
                                                  partitionCount: Option[Int],
                                                  metadata: Option[VerticaReadMetadata]
                                                ) extends ReadConfig {
  private var pushdownFilters: List[PushdownFilter] = Nil
  private var requiredSchema: StructType = StructType(Nil)

  def setPushdownFilters(pushdownFilters: List[PushdownFilter]): Unit = {
    this.pushdownFilters = pushdownFilters
  }

  def setRequiredSchema(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  def getPushdownFilters: List[PushdownFilter] = this.pushdownFilters
  def getRequiredSchema: StructType = this.requiredSchema
}
