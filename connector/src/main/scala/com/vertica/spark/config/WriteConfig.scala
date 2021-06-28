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

import cats.implicits.catsSyntaxValidatedIdBinCompat0
import com.vertica.spark.datasource.core.DSConfigSetupUtils.ValidationResult
import com.vertica.spark.util.error.UnquotedSemiInColumns
import org.apache.spark.sql.types.StructType

/**
 * Interface for configuration of a wrtie (to Vertica) operation.
 */
trait WriteConfig {
  /**
   * Setter for marking this write operation to overwrite any existing table by the name specified.
   *
   * If not set, default is to append to the existing table.
   */
  def setOverwrite(overwrite: Boolean): Unit
}

/**
 * Column list, validated on the type level to escape unquoted semicolons (SQL Injection prevention)
 */
class ValidColumnList private (value: String) extends Serializable {
  override def toString: String = value
}

object ValidColumnList {
  private def checkStringForUnquotedSemicolon(str: String): Boolean = {
    var i = 0
    var inQuote = false
    var isUnquotedSemi = false
    for(c <- str) {
      if(c == '"') inQuote = !inQuote
      if(c == ';' && !inQuote) isUnquotedSemi = true
      i += 1
    }

    isUnquotedSemi
  }

  final def apply(value: String): ValidationResult[Option[ValidColumnList]] = {
    if (!checkStringForUnquotedSemicolon(value)) {
      Some(new ValidColumnList(value)).validNec
    } else {
      UnquotedSemiInColumns().invalidNec
    }
  }
}

/**
 * Configuration for a write operation using a distributed filesystem as an intermediary.
 *
 * @param jdbcConfig Configuration for the JDBC connection used to communicate with Vertica.
 * @param fileStoreConfig Configuration for the intermediary filestore used to stage data between Spark and Vertica.
 * @param tablename Tablename to write to
 * @param schema Schema of the data being written
 * @param strlen Length to use for strings when creating tables to write strings to.
 * @param targetTableSql Optional SQL statment to run before the write operation, used for custom table creation.
 * @param copyColumnList Optional list of columns to use in copy statement.
 * @param sessionId Unqiue identifier for this write operation.
 * @param failedRowPercentTolerance Value between 0 and 1, percent of rows that are allowed to fail before the operation fails.
 * @param mergeKey Key that two data sets are joined on in order to execute a merge statement
 */
final case class DistributedFilesystemWriteConfig(jdbcConfig: JDBCConfig,
                                                  fileStoreConfig: FileStoreConfig,
                                                  tablename: TableName,
                                                  schema: StructType,
                                                  strlen: Long,
                                                  targetTableSql: Option[String],
                                                  copyColumnList: Option[ValidColumnList],
                                                  sessionId: String,
                                                  failedRowPercentTolerance: Float,
                                                  filePermissions: ValidFilePermissions,
                                                  mergeKey: Option[String]
                                                 ) extends WriteConfig {
  private var overwrite: Boolean = false

  def setOverwrite(overwrite: Boolean): Unit = this.overwrite = overwrite
  def isOverwrite: Boolean = overwrite
}

