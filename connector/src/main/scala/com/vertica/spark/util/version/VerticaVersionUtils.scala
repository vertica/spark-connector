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

import com.vertica.spark.datasource.jdbc.{JdbcLayerInterface, JdbcUtils}
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.schema.ComplexTypesSchemaTools
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

import java.sql.ResultSet
import scala.util.{Failure, Success, Try}

// scalastyle:off magic.number
object VerticaVersionUtils {
  val VERTICA_11: Version = Version(11)
  val VERTICA_11_1: Version = Version(11,1)
  val VERTICA_11_1_1: Version = Version(11,1,1)
  // Should always be the latest major release.
  val VERTICA_DEFAULT: Version = Version(12)
  val complexTypeUtils = new ComplexTypesSchemaTools()

  /*
  * Query for a Vertica version. Return the default version on any error.
  * */
  def getVersionOrDefault(jdbcLayer: JdbcLayerInterface): Version = getVersion(jdbcLayer).getOrElse(VERTICA_DEFAULT)

  /**
   * Query for a Vertica version
   * */
  def getVersion(jdbcLayer: JdbcLayerInterface): ConnectorResult[Version] = JdbcUtils.queryAndNext("SELECT version();", jdbcLayer, extractVersion)

  private def extractVersion(rs: ResultSet): ConnectorResult[Version] = {
    val versionString = rs.getString(1)
    val pattern = ".*v([0-9]+)\\.([0-9]+)\\.([0-9])+-([0-9]+).*".r
    Try {
      val pattern(major, minor, service, hotfix) = versionString
      Version(major.toInt, minor.toInt, service.toInt, hotfix.toInt)
    } match {
      case Failure(_) => Left(UnrecognizedVerticaVersionString(versionString))
      case Success(version) => Right(version)
    }
  }

  def checkSchemaTypesWriteSupport(schema: StructType, version: Version, toInternalTable: Boolean): ConnectorResult[Unit] = {
    val (nativeCols, complexTypeCols) = complexTypeUtils.filterColumnTypes(schema)
    val complexTypeFound = complexTypeCols.nonEmpty
    val nativeArrayCols = nativeCols.filter(_.dataType.isInstanceOf[ArrayType])
    if (version.major <= 9) {
        if(complexTypeFound) {
          Left(ComplexTypeWriteNotSupported(complexTypeCols, version.toString))
        } else if (nativeArrayCols.nonEmpty) {
          Left(NativeArrayWriteNotSupported(nativeArrayCols, version.toString))
        } else {
          Right()
        }
    } else if (version.major == 10) {
      if(complexTypeFound) {
        // Even though Vertica 10.x supports Complex types in external tables, it seems that
        // the underlying protocol does not support complex type, thus throwing an error
        // regardless of internal or external tables
        Left(ComplexTypeWriteNotSupported(complexTypeCols, version.toString))
      } else {
        Right()
      }
    } else {
        if(toInternalTable && complexTypeCols.exists(_.dataType.isInstanceOf[MapType])) {
          Left(InternalMapNotSupported())
        } else {
          Right()
        }
    }
  }

  def checkSchemaTypesReadSupport(schema: StructType, version: Version): ConnectorResult[Unit] = {
    val (nativeCols, complexTypeCols) = complexTypeUtils.filterColumnTypes(schema)
    val nativeArrayCols = nativeCols.filter(_.dataType.isInstanceOf[ArrayType])
    if (version.major <= 10) {
      if (complexTypeCols.nonEmpty) {
        Left(ComplexTypeReadNotSupported(complexTypeCols, version.toString))
      } else if (nativeArrayCols.nonEmpty) {
        Left(NativeArrayReadNotSupported(nativeArrayCols, version.toString))
      } else {
        Right()
      }
    } else if (version.lesserOrEqual(VERTICA_11_1)) {
      if (complexTypeCols.nonEmpty) {
        Left(ComplexTypeReadNotSupported(complexTypeCols, version.toString))
      } else {
        Right()
      }
    } else {
      Right()
    }
  }

  /**
   * Export to Json was added in Vertica 11.1.1
   * */
  def checkJsonSupport(version: Version): ConnectorResult[Unit] =
    if(version.lessThan(VERTICA_11_1_1)){
      Left(ExportToJsonNotSupported(version.toString))
    } else {
      Right()
    }
}


