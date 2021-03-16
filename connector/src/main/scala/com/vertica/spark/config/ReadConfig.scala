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
import com.vertica.spark.datasource.v2.PushdownFilter
import com.vertica.spark.util.error.{InvalidFilePermissions, UnquotedSemiInColumns}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

trait ReadConfig extends GenericConfig {
  def setPushdownFilters(pushdownFilters: List[PushdownFilter]): Unit
  def setRequiredSchema(requiredSchema: StructType): Unit
  def getRequiredSchema: StructType
}

class ValidFilePermissions (value: String) extends Serializable {
  override def toString: String = value
}

object ValidFilePermissions {

  // octal format, ie 777 or 0750
  private def isValidOctal(str: String): Boolean = {
    str.length <= 4 && str.forall(_.isDigit)
  }

  // user-group-other format
  private def isValidLetterPerms(str: String): Boolean = {
    str.forall(c => "crwx-".contains(c))
  }

  final def apply(value: String): ValidationResult[ValidFilePermissions] = {
    val validOctal = isValidOctal(value)
    val validLetterParams = isValidLetterPerms(value)
    if (validOctal || validLetterParams) {
      new ValidFilePermissions(value).validNec
    } else {
      InvalidFilePermissions().invalidNec
    }
  }
}

final case class DistributedFilesystemReadConfig(
                                                  override val logLevel: Level,
                                                  jdbcConfig: JDBCConfig,
                                                  fileStoreConfig: FileStoreConfig,
                                                  tablename: TableName,
                                                  partitionCount: Option[Int],
                                                  metadata: Option[VerticaReadMetadata],
                                                  filePermissions: ValidFilePermissions
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
