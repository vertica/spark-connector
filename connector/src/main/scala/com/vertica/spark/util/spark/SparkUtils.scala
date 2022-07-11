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

package com.vertica.spark.util.spark

import com.vertica.spark.util.version.Version
import org.apache.spark.sql.SparkSession

import scala.util.Try

object SparkUtils {
  def getVersionString: Option[String] = SparkSession.getActiveSession.map(_.version)

  def getVersion(versionStr: Option[String]): Option[Version] = versionStr match {
    case Some(str) =>
      val regex = "([0-9]+)\\.([0-9]+)\\.([0-9]+)(.*)".r
      Try{
        val regex(major, minor, service, _) = str
        Some(Version(major.toInt, minor.toInt, service.toInt))
      }.getOrElse(None)
    case None => None
  }
}
