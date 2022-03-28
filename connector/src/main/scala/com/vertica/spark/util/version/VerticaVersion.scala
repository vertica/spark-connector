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
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.{ComplexArrayReadNotSupported, ComplexArrayWritingNotSupported, ComplexTypeNotSupported, ComplexTypeColumnsNotSupported, ConnectorError, ErrorList, NoResultError}
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

import java.util
import scala.util.Try


object VerticaVersionUtils {

  private val logger = LogProvider.getLogger(this.getClass)
  private val version: Option[VerticaVersion] = None
  // Should always be the latest major release.
  // scalastyle:off
  val LATEST: VerticaVersion = VerticaVersion(11)

  /**
   * Query and cache Vertica version. Return the default version on any error.
   * */
  def getVersion(jdbcLayer: JdbcLayerInterface): VerticaVersion =
    JdbcUtils.queryAndNext("SELECT version();", jdbcLayer, (rs) => {
      val verticaVersion = extractVersion(rs.getString(1))
      logger.info("VERTICA VERSION: " + verticaVersion)
      Right(verticaVersion)
    }, (query) => {
      logger.error("Failed to query for version number. Defaults to " + LATEST)
      Left(NoResultError(query))
    }).getOrElse(LATEST)

  private def extractVersion(str: String): VerticaVersion = {
    val pattern = ".*v([0-9]+)\\.([0-9]+)\\.([0-9])+-([0-9]+).*".r
    Try{
      val pattern(major, minor, service, hotfix) = str
      VerticaVersion(major.toInt, minor.toInt, service.toInt, hotfix.toInt)
    }.getOrElse(LATEST)
  }

  def checkSchemaTypesWriteSupport(schema: StructType, writingToExternal: Boolean, version: VerticaVersion): ConnectorResult[Unit] = {
    if(version.major > 10) {
      Right()
    } else if (version.major < 10) {
      checkCTSupportV9AndLess(schema, version)
    } else {
      checkTypesWriteSupportV10(schema, writingToExternal, version)
    }
  }

  private def checkTypesWriteSupportV10(schema: StructType, writingToExternal: Boolean, version: VerticaVersion): ConnectorResult[Unit] = {
    val errorsList  = schema.foldLeft(List[ConnectorError]())((accumErrors, field) => {
      field.dataType match {
        case ArrayType(elementType,_) => elementType match {
          case ArrayType(_,_) | StructType(_) | MapType(_,_,_) =>
            if(!writingToExternal) accumErrors :+ ComplexArrayWritingNotSupported(field.name, version.toString)
            else accumErrors
          case _ => accumErrors
        }
        case StructType(_) | MapType(_,_,_) => accumErrors :+ ComplexTypeNotSupported(field.name, field.dataType.toString, version.toString)
        case _ => accumErrors
      }
    })
    if(errorsList.isEmpty) {
      Right()
    }else{
      Left(ErrorList(NonEmptyList(errorsList.head, errorsList.tail)))
    }
  }

  def checkSchemaTypesReadSupport(schema: StructType, version: VerticaVersion): ConnectorResult[Unit] = {
    if(version.major > 10) {
      checkCTReadSupportV11AndHigher(schema, version)
    } else if (version.major < 10) {
      checkCTSupportV9AndLess(schema, version)
    } else {
      checkCTReadSupportV10(schema, version)
    }
  }

  private def checkCTReadSupportV11AndHigher(schema: StructType, version: VerticaVersion): ConnectorResult[Unit] = {
    val errorsFound = schema.foldLeft(List[ConnectorError]())((accumErrors, field) => {
      field.dataType match {
        case ArrayType(elementType,_) => elementType match {
          case ArrayType(_,_) | StructType(_) | MapType(_,_,_) =>
            accumErrors :+ ComplexArrayReadNotSupported(field.name, version.toString)
          case _ => accumErrors
        }
        case StructType(_) | MapType(_,_,_) => accumErrors :+ ComplexTypeNotSupported(field.name, field.dataType.toString, version.toString)
        case _ => accumErrors
      }
    })
    if(errorsFound.isEmpty) {
      Right()
    }else{
      Left(ErrorList(NonEmptyList(errorsFound.head, errorsFound.tail)))
    }
  }

  private def checkCTReadSupportV10(schema: StructType, version: VerticaVersion): ConnectorResult[Unit] = {
    val errorsFound = schema.foldLeft(List[ConnectorError]())((accumErrors, field) => {
      field.dataType match {
        case ArrayType(elementType,_) => elementType match {
          case ArrayType(_,_) | StructType(_) | MapType(_,_,_) =>
            accumErrors :+ ComplexArrayReadNotSupported(field.name, version.toString)
          case _ => accumErrors
        }
        case StructType(_) | MapType(_,_,_) => accumErrors :+ ComplexTypeNotSupported(field.name, field.dataType.toString, version.toString)
        case _ => accumErrors
      }
    })
    if(errorsFound.isEmpty) {
      Right()
    }else{
      Left(ErrorList(NonEmptyList(errorsFound.head, errorsFound.tail)))
    }
  }

  private def checkCTSupportV9AndLess(schema: StructType, version: VerticaVersion): ConnectorResult[Unit] = {
    val complexCols = schema.foldLeft(List[StructField]())((complexCols, field) => {
      field.dataType match {
        case ArrayType(_,_) | StructType(_) => complexCols :+ field
        case _ => complexCols
      }
    })
    if(complexCols.isEmpty) {
      Right()
    }else{
      Left(ComplexTypeColumnsNotSupported(complexCols, version.toString))
    }
  }
}

case class VerticaVersion(major: Int, minor: Int = 0, servicePack: Int = 0, hotfix: Int = 0) extends Ordered[VerticaVersion] {
  override def toString: String = s"${major}.${minor}.${servicePack}-${hotfix}"

  override def compare(that: VerticaVersion): Int =
    (this.major * 1000 + this.minor * 100 + this.servicePack * 10 + this.hotfix) -
      (that.major * 1000 + that.minor * 100 + that.servicePack * 10 + that.hotfix)
}
