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

case class SensitiveArg[+T](visibility: Visibility, origin: ArgOrigin, arg: T) {
  override def toString: String = this.visibility match {
    case Secret => s"SensitiveArg(${origin.toString}, *****)"
    case Visible => s"SensitiveArg(${origin.toString}, ${arg.toString})"
  }
}

case class AWSAuth(accessKeyId: SensitiveArg[String], secretAccessKey: SensitiveArg[String])

case class AWSOptions(
                       awsAuth: Option[AWSAuth],
                       awsRegion: Option[SensitiveArg[String]],
                       awsSessionToken: Option[SensitiveArg[String]],
                       awsCredentialsProvider: Option[SensitiveArg[String]],
                       awsEndpoint: Option[SensitiveArg[String]],
                       enableSSL: Option[SensitiveArg[String]],
                       enablePathStyle: Option[SensitiveArg[String]])

case class VerticaGCSAuth(accessKeyId: SensitiveArg[String], accessKeySecret: SensitiveArg[String])

case class GCSServiceAccountAuth(serviceAccKeyId: SensitiveArg[String], serviceAccKeySecret: SensitiveArg[String], serviceAccEmail: SensitiveArg[String])

case class GCSOptions(gcsAuth: Option[VerticaGCSAuth], gcsKeyFile: Option[SensitiveArg[String]], gcsServiceAccount: Option[GCSServiceAccountAuth])

/**
 * Represents configuration for a filestore used by the connector.
 *
 * There is not currently much user configuration for the filestore beyond the address to connect to.
 * @param baseAddress Address to use in the intermediate filesystem
 * @param sessionId Unique id for a given connector operation
 * @param preventCleanup A boolean that prevents cleanup if specified to true
 * @param awsOptions Options for configuring AWS S3 storage
 * @param gcsOptions Options for configuring Google Cloud Storage
 */
final case class FileStoreConfig(baseAddress: String, sessionId: String, preventCleanup: Boolean, awsOptions: AWSOptions, gcsOptions: GCSOptions) {
  val defaultFS =
    if(baseAddress.startsWith("/")) {
      SparkSession.getActiveSession match {
        case Some(session) =>
          val hadoopConf = session.sparkContext.hadoopConfiguration
          val filepath = Option(hadoopConf.get("fs.defaultFS")) match {
            case Some(path) => Right(path)
            case None => Left(HDFSConfigError().context("No fs.defaultFS value supplied in HDFS config"))
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
