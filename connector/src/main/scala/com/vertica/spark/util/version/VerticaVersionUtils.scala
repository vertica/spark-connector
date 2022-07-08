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
package com.vertica.spark.util.version

import com.vertica.spark.config.LogProvider
import com.vertica.spark.datasource.jdbc.{JdbcLayerInterface, JdbcUtils}
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.schema.ComplexTypesSchemaTools
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

import scala.util.Try


object VerticaVersionUtils {

  private val logger = LogProvider.getLogger(this.getClass)
  private val version: Option[Version] = None
  // Should always be the latest major release.
  // scalastyle:off
  val VERRTICA_LATEST: Version = Version(11)
  val complexTypeUtils = new ComplexTypesSchemaTools()

  /**
   * Query and cache Vertica version. Return the default version on any error.
   * */
  def getVersion(jdbcLayer: JdbcLayerInterface): Version =
    JdbcUtils.queryAndNext("SELECT version();", jdbcLayer, (rs) => {
      val verticaVersion = extractVersion(rs.getString(1))
      logger.info("VERTICA VERSION: " + verticaVersion)
      Right(verticaVersion)
    }, (query) => {
      logger.error("Failed to query for version number. Defaults to " + VERRTICA_LATEST)
      Left(NoResultError(query))
    }).getOrElse(VERRTICA_LATEST)

  private def extractVersion(str: String): Version = {
    val pattern = ".*v([0-9]+)\\.([0-9]+)\\.([0-9])+-([0-9]+).*".r
    Try{
      val pattern(major, minor, service, hotfix) = str
      Version(major.toInt, minor.toInt, service.toInt, hotfix.toInt)
    }.getOrElse(VERRTICA_LATEST)
  }

  def checkSchemaTypesWriteSupport(schema: StructType, version: Version, toInternalTable: Boolean): ConnectorResult[Unit] = {
    val (nativeCols, complexTypeCols) = complexTypeUtils.filterColumnTypes(schema)
    val complexTypeFound = complexTypeCols.nonEmpty
    val nativeArrayCols = nativeCols.filter(_.dataType.isInstanceOf[ArrayType])
    if (version.major <= 9) {
        if(complexTypeFound)
          Left(ComplexTypeWriteNotSupported(complexTypeCols, version.toString))
        else if (nativeArrayCols.nonEmpty)
          Left(NativeArrayWriteNotSupported(nativeArrayCols, version.toString))
        else
          Right()
    } else if (version.major == 10) {
      if(complexTypeFound)
        // Even though Vertica 10.x supports Complex types in external tables, it seems that
        // the underlying protocol does not support complex type, thus throwing an error
        // regardless of internal or external tables
        Left(ComplexTypeWriteNotSupported(complexTypeCols, version.toString))
      else Right()
    } else {
        if(toInternalTable && complexTypeCols.exists(_.dataType.isInstanceOf[MapType]))
          Left(InternalMapNotSupported())
        else
          Right()
    }
  }

  def checkSchemaTypesReadSupport(schema: StructType, version: Version): ConnectorResult[Unit] = {
    val (nativeCols, complexTypeCols) = complexTypeUtils.filterColumnTypes(schema)
    val nativeArrayCols = nativeCols.filter(_.dataType.isInstanceOf[ArrayType])
    if (version.major <= 10) {
      if (complexTypeCols.nonEmpty)
        Left(ComplexTypeReadNotSupported(complexTypeCols, version.toString))
      else if (nativeArrayCols.nonEmpty)
        Left(NativeArrayReadNotSupported(nativeArrayCols, version.toString))
      else Right()
    } else if (version.lesserOrEqual(Version(11, 1))) {
      if (complexTypeCols.nonEmpty)
        Left(ComplexTypeReadNotSupported(complexTypeCols, version.toString))
      else
        Right()
    } else {
      Right()
    }
  }

  /**
   * Export to Json was added in Vertica 11.1.1
   * */
  def checkJsonSupport(version: Version): ConnectorResult[Unit] =
    if(version.lessThan(Version(11,1,1))){
      Left(ExportToJsonNotSupported(version.toString))
    }  else {
      Right()
    }

}


