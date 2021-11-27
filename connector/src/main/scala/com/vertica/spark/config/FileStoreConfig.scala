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

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys
import com.vertica.spark.util.error.{NoSparkSessionFound, HDFSConfigError}
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.hadoop.conf.Configuration

sealed trait ArgOrigin
case object EnvVar extends ArgOrigin
case object SparkConf extends ArgOrigin
case object ConnectorOption extends ArgOrigin

sealed trait Visibility
case object Secret extends Visibility
case object Visible extends Visibility

case class AWSArg[+T](visibility: Visibility, origin: ArgOrigin, arg: T) {
  override def toString: String = this.visibility match {
    case Secret => s"AWSArg(${origin.toString}, *****)"
    case Visible => s"AWSArg(${origin.toString}, ${arg.toString})"
  }
}

case class AWSAuth(accessKeyId: AWSArg[String], secretAccessKey: AWSArg[String])

case class AWSOptions(
                       awsAuth: Option[AWSAuth],
                       awsRegion: Option[AWSArg[String]],
                       awsSessionToken: Option[AWSArg[String]],
                       awsCredentialsProvider: Option[AWSArg[String]],
                       awsEndpoint: Option[AWSArg[String]],
                       enableSSL: Option[AWSArg[String]])

/**
 * Represents configuration for a filestore used by the connector.
 *
 * There is not currently much user configuration for the filestore beyond the address to connect to.
 * @param baseAddress Address to use in the intermediate filesystem
 * @param sessionId Unique id for a given connector operation
 */
final case class FileStoreConfig(baseAddress: String, sessionId: String, awsOptions: AWSOptions) {
  val defaultFS =
    if(baseAddress.startsWith("/")) {
      SparkSession.getActiveSession match {
        case Some(session) =>
          val hadoopConf = session.sparkContext.hadoopConfiguration
          val filepath = Option(hadoopConf.get("fs.defaultFS")) match {
            case Some(path) => Right(path)
            case None => Left(HDFSConfigError())
          }
          filepath
        case None => Left(NoSparkSessionFound())
      }
    }
    else {
      None
    }

  val newBaseAddress = defaultFS match {
    case Right(prefix) => prefix + baseAddress
    case _ => baseAddress
  }

  def address: String = {
    val delimiter = if(baseAddress.takeRight(1) == "/" || baseAddress.takeRight(1) == "\\") "" else "/"
    // Create unique directory for session
    baseAddress.stripSuffix(delimiter) + delimiter + sessionId
    newBaseAddress.stripSuffix(delimiter) + delimiter + sessionId
  }

  def externalTableAddress: String = {
    val delimiter = if(baseAddress.takeRight(1) == "/" || baseAddress.takeRight(1) == "\\") "" else "/"
    // URL for directory without session ID
    baseAddress.stripSuffix(delimiter) + delimiter
    newBaseAddress.stripSuffix(delimiter) + delimiter
  }
}
