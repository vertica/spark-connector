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

import cats.data.NonEmptyList
import com.vertica.spark.config.LogProvider
import com.vertica.spark.datasource.jdbc.{JdbcLayerInterface, JdbcUtils}
import com.vertica.spark.util.complex.ComplexTypeUtils
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.{ComplexTypeReadNotSupported, ComplexTypeWriteNotSupported, InternalMapNotSupported, NativeArrayReadNotSupported, NativeArrayWriteNotSupported, NoResultError}
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

import java.util
import scala.util.Try


object VerticaVersionUtils {

  private val logger = LogProvider.getLogger(this.getClass)
  private val version: Option[VerticaVersion] = None
  // Should always be the latest major release.
  // scalastyle:off
  val VERRTICA_LATEST: VerticaVersion = VerticaVersion(11)
  val complexTypeUtils = new ComplexTypeUtils()
  /**
   * Query and cache Vertica version. Return the default version on any error.
   * */
  def getVersion(jdbcLayer: JdbcLayerInterface): VerticaVersion =
    JdbcUtils.queryAndNext("SELECT version();", jdbcLayer, (rs) => {
      val verticaVersion = extractVersion(rs.getString(1))
      logger.info("VERTICA VERSION: " + verticaVersion)
      Right(verticaVersion)
    }, (query) => {
      logger.error("Failed to query for version number. Defaults to " + VERRTICA_LATEST)
      Left(NoResultError(query))
    }).getOrElse(VERRTICA_LATEST)

  private def extractVersion(str: String): VerticaVersion = {
    val pattern = ".*v([0-9]+)\\.([0-9]+)\\.([0-9])+-([0-9]+).*".r
    Try{
      val pattern(major, minor, service, hotfix) = str
      VerticaVersion(major.toInt, minor.toInt, service.toInt, hotfix.toInt)
    }.getOrElse(VERRTICA_LATEST)
  }

  def checkSchemaTypesWriteSupport(schema: StructType, version: VerticaVersion, toInternalTable: Boolean): ConnectorResult[Unit] = {
    val (nativeCols, complexTypeCols) = complexTypeUtils.getComplexTypeColumns(schema)
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
        // regardless of internal of external tables
        Left(ComplexTypeWriteNotSupported(complexTypeCols, version.toString))
      else Right()
    } else {
        if(toInternalTable && complexTypeCols.exists(_.dataType.isInstanceOf[MapType]))
          Left(InternalMapNotSupported())
        else
          Right()
    }
  }

  def checkSchemaTypesReadSupport(schema: StructType, version: VerticaVersion): ConnectorResult[Unit] = {
    val (nativeCols, complexTypeCols) = complexTypeUtils.getComplexTypeColumns(schema)
    val nativeArrayCols = nativeCols.filter(_.dataType.isInstanceOf[ArrayType])
    if (version.major <= 10) {
      if (complexTypeCols.nonEmpty)
        Left(ComplexTypeReadNotSupported(complexTypeCols, version.toString))
      else if (nativeArrayCols.nonEmpty)
        Left(NativeArrayReadNotSupported(nativeArrayCols, version.toString))
      else Right()
    } else {
      // As of Vertica 11.x the EXPORT function does not support exporting complex types to parquet.
      if (complexTypeCols.nonEmpty)
        Left(ComplexTypeReadNotSupported(complexTypeCols, version.toString))
      else
        Right()
    }
  }
}

case class VerticaVersion(major: Int, minor: Int = 0, servicePack: Int = 0, hotfix: Int = 0) extends Ordered[VerticaVersion] {
  override def toString: String = s"${major}.${minor}.${servicePack}-${hotfix}"

  override def compare(that: VerticaVersion): Int =
    (this.major * 1000 + this.minor * 100 + this.servicePack * 10 + this.hotfix) -
      (that.major * 1000 + that.minor * 100 + that.servicePack * 10 + that.hotfix)
}
