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
import ch.qos.logback.classic.Level
import com.vertica.spark.datasource.core.DSConfigSetupUtils.ValidationResult
import com.vertica.spark.util.error.ConnectorError
import com.vertica.spark.util.error.ConnectorErrorType.UnquotedSemiInColumns
import org.apache.spark.sql.types.StructType

trait WriteConfig extends GenericConfig {
  def setOverwrite(overwrite: Boolean): Unit
}

class ValidColumnList private (value: String) {
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
      Some((new ValidColumnList(value))).validNec
    } else {
      ConnectorError(UnquotedSemiInColumns).invalidNec
    }
  }
}

final case class DistributedFilesystemWriteConfig(override val logLevel: Level,
                                                  jdbcConfig: JDBCConfig,
                                                  fileStoreConfig: FileStoreConfig,
                                                  tablename: TableName,
                                                  schema: StructType,
                                                  strlen: Long,
                                                  targetTableSql: Option[String],
                                                  copyColumnList: Option[ValidColumnList],
                                                  sessionId: String,
                                                  failedRowPercentTolerance: Float
                                                 ) extends WriteConfig {
  private var overwrite: Boolean = false

  def setOverwrite(overwrite: Boolean): Unit = this.overwrite = overwrite
  def isOverwrite: Boolean = overwrite
}

