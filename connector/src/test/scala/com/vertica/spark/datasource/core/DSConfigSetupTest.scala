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

package com.vertica.spark.datasource.core

import cats.data.Validated.{Invalid, Valid}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import com.vertica.spark.config._
import com.vertica.spark.datasource.core.factory.VerticaPipeFactoryInterface
import org.scalamock.scalatest.MockFactory
import com.vertica.spark.util.error._
import com.vertica.spark.datasource.v2.DummyReadPipe
import org.apache.spark.sql.types._

class DSConfigSetupTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {
  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
  }


  // Parses config expecting success
  // Calling test with fail if an error is returned
  def parseCorrectInitConfig(opts : Map[String, String], dsReadConfigSetup: DSReadConfigSetup) : ReadConfig = {
    val readConfig : ReadConfig = dsReadConfigSetup.validateAndGetConfig(opts) match {
      case Invalid(_) =>
        fail
        mock[ReadConfig]
      case Valid(config) =>
        config
    }
    readConfig
  }
  def parseCorrectInitConfig(opts : Map[String, String], dsWriteConfigSetup: DSWriteConfigSetup) : WriteConfig = {
    val writeConfig : WriteConfig = dsWriteConfigSetup.validateAndGetConfig(opts) match {
      case Invalid(_) =>
        fail
        mock[WriteConfig]
      case Valid(config) =>
        config
    }
    writeConfig
  }

  // Parses config expecting an error
  // Calling test will fail if the config is parsed without error
  def parseErrorInitConfig(opts : Map[String, String], dsReadConfigSetup: DSReadConfigSetup) : Seq[ConnectorError] = {
    dsReadConfigSetup.validateAndGetConfig(opts) match {
      case Invalid(errList) => errList.toNonEmptyList.toList
      case Valid(_) =>
        fail
        List[ConnectorError]()
    }
  }
  def parseErrorInitConfig(opts : Map[String, String], dsWriteConfigSetup: DSWriteConfigSetup) : Seq[ConnectorError] = {
    dsWriteConfigSetup.validateAndGetConfig(opts) match {
      case Invalid(errList) => errList.toNonEmptyList.toList
      case Valid(_) =>
        fail
        List[ConnectorError]()
    }
  }


  it should "parse a valid read config" in {
    val opts = Map("host" -> "1.1.1.1",
                   "port" -> "1234",
                   "db" -> "testdb",
                   "user" -> "user",
                   "password" -> "password",
                   "table" -> "tbl",
                   "staging_fs_url" -> "hdfs://test:8020/tmp/test"
    )

    // Set mock pipe
    val mockPipe = mock[DummyReadPipe]
    (mockPipe.getMetadata _).expects().returning(Right(VerticaReadMetadata(new StructType))).once()
    val mockPipeFactory = mock[VerticaPipeFactoryInterface]
    (mockPipeFactory.getReadPipe _).expects(*).returning(mockPipe)

    val dsReadConfigSetup = new DSReadConfigSetup(mockPipeFactory)

    parseCorrectInitConfig(opts, dsReadConfigSetup) match {
      case config: DistributedFilesystemReadConfig =>
        assert(config.jdbcConfig.host == "1.1.1.1")
        assert(config.jdbcConfig.port == 1234)
        assert(config.jdbcConfig.db == "testdb")
        assert(config.tableSource.asInstanceOf[TableName].getFullTableName == "\"tbl\"")
        config.metadata match {
          case Some(metadata) => assert(metadata.schema == new StructType())
          case None => fail
        }
    }
  }

  it should "Return several parsing errors on read" in {
    // Should be one error from the jdbc parser for the port and one for the missing log level
    val opts = Map("host" -> "1.1.1.1",
                   "db" -> "testdb",
                   "port" -> "asdf",
                   "user" -> "user",
                   "password" -> "password",
                   "table" -> "tbl",
                   "staging_fs_url" -> "hdfs://test:8020/tmp/test",
                   "num_partitions" -> "foo"
    )

    val dsReadConfigSetup = new DSReadConfigSetup(mock[VerticaPipeFactoryInterface])

    val errSeq = parseErrorInitConfig(opts, dsReadConfigSetup)
    assert(errSeq.size == 2)
    assert(errSeq.contains(InvalidPortError()))
    assert(errSeq.contains(InvalidPartitionCountError()))
  }

  it should "Return error when there's a problem retrieving metadata" in {

    val opts = Map("host" -> "1.1.1.1",
                   "port" -> "1234",
                   "db" -> "testdb",
                   "user" -> "user",
                   "password" -> "password",
                   "table" -> "tbl",
                   "staging_fs_url" -> "hdfs://test:8020/tmp/test"
    )

    // Set mock pipe
    val mockPipe = mock[DummyReadPipe]
    (mockPipe.getMetadata _).expects().returning(Left(SchemaDiscoveryError())).once()
    val mockPipeFactory = mock[VerticaPipeFactoryInterface]
    (mockPipeFactory.getReadPipe _).expects(*).returning(mockPipe)

    val dsReadConfigSetup = new DSReadConfigSetup(mockPipeFactory)

    val errSeq = parseErrorInitConfig(opts, dsReadConfigSetup)
    assert(errSeq.size == 1)
    assert(errSeq.map(_.getError).contains(SchemaDiscoveryError()))
  }

  it should "parse a valid write config" in {
    val opts = Map(
      "host" -> "1.1.1.1",
      "port" -> "1234",
      "db" -> "testdb",
      "user" -> "user",
      "password" -> "password",
      "table" -> "tbl",
      "staging_fs_url" -> "hdfs://test:8020/tmp/test"
    )

    // Set mock pipe
    val mockPipeFactory = mock[VerticaPipeFactoryInterface]

    val dsWriteConfigSetup = new DSWriteConfigSetup(Some(new StructType), mockPipeFactory)

    parseCorrectInitConfig(opts, dsWriteConfigSetup) match {
      case config: DistributedFilesystemWriteConfig =>
        assert(config.jdbcConfig.host == "1.1.1.1")
        assert(config.jdbcConfig.port == 1234)
        assert(config.jdbcConfig.db == "testdb")
        assert(config.tablename.getFullTableName == "\"tbl\"")
    }
  }

  it should "Return several parsing errors on write" in {
    val opts = Map(
      "host" -> "1.1.1.1",
      "db" -> "testdb",
      "port" -> "asdf",
      "user" -> "user",
      "password" -> "password",
      "table" -> "tbl",
      "failed_rows_percent_tolerance" -> "2.00",
      "staging_fs_url" -> "hdfs://test:8020/tmp/test"
    )

    // Set mock pipe
    val mockPipeFactory = mock[VerticaPipeFactoryInterface]

    val dsWriteConfigSetup = new DSWriteConfigSetup(Some(new StructType), mockPipeFactory)

    val errSeq = parseErrorInitConfig(opts, dsWriteConfigSetup)
    assert(errSeq.size == 2)
    assert(errSeq.map(_.getError).contains(InvalidPortError()))
    assert(errSeq.map(_.getError).contains(InvalidFailedRowsTolerance()))

  }

  it should "get the AWS access key id and secret access key from environment variables" in {
    val opts = Map(
      "host" -> "1.1.1.1",
      "port" -> "1234",
      "db" -> "testdb",
      "user" -> "user",
      "password" -> "password",
      "table" -> "tbl",
      "staging_fs_url" -> "hdfs://test:8020/tmp/test"
    )

    // Set mock pipe
    val mockPipeFactory = mock[VerticaPipeFactoryInterface]

    val dsWriteConfigSetup = new DSWriteConfigSetup(Some(new StructType), mockPipeFactory)

    parseCorrectInitConfig(opts, dsWriteConfigSetup) match {
      case config: DistributedFilesystemWriteConfig =>
        config.fileStoreConfig.awsOptions.awsAuth match {
          case Some(auth) =>
            assert(auth.accessKeyId == "test")
            assert(auth.secretAccessKey == "foo")
          case None => fail("Failed to get AWS Auth from the environment variables")
        }
    }
  }

  it should "get the AWS region" in {
    val opts = Map(
      "host" -> "1.1.1.1",
      "port" -> "1234",
      "db" -> "testdb",
      "user" -> "user",
      "password" -> "password",
      "table" -> "tbl",
      "staging_fs_url" -> "hdfs://test:8020/tmp/test",
      "aws_region" -> "us-east-1"
    )

    // Set mock pipe
    val mockPipeFactory = mock[VerticaPipeFactoryInterface]

    val dsWriteConfigSetup = new DSWriteConfigSetup(Some(new StructType), mockPipeFactory)

    parseCorrectInitConfig(opts, dsWriteConfigSetup) match {
      case config: DistributedFilesystemWriteConfig =>
        config.fileStoreConfig.awsOptions.awsRegion match {
          case Some(region) => assert(region == "us-east-1")
          case None => fail("Failed to get AWS region from the configuration")
        }
    }
  }
}
