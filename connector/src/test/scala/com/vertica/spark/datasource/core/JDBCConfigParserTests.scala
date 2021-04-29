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
import cats.implicits._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import com.vertica.spark.config.{BasicJdbcAuth, KerberosAuth}
import org.scalamock.scalatest.MockFactory
import com.vertica.spark.util.error.{DbMissingError, HostMissingError, PasswordMissingError, SSLFlagParseError, UserMissingError}


class JDBCConfigParserTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  it should "parse the JDBC config" in {
    val opts = Map(
                   "host" -> "1.1.1.1",
                   "port" -> "1234",
                   "db" -> "testdb",
                   "user" -> "user",
                   "password" -> "password"
    )

    DSConfigSetupUtils.validateAndGetJDBCConfig(opts) match {
      case Invalid(_) =>
        fail
      case Valid(jdbcConfig) =>
        assert(jdbcConfig.host == "1.1.1.1")
        assert(jdbcConfig.port == 1234)
        assert(jdbcConfig.db == "testdb")
        assert(jdbcConfig.auth.asInstanceOf[BasicJdbcAuth].username == "user")
        assert(jdbcConfig.auth.asInstanceOf[BasicJdbcAuth].password == "password")
    }
  }

  it should "parse the JDBC config with Kerberos" in {
    val opts = Map(
      "host" -> "1.1.1.1",
      "port" -> "1234",
      "db" -> "testdb",
      "user" -> "user",
      "kerberos_service_name" -> "vertica",
      "kerberos_host_name" -> "vertica.example.com",
      "jaas_config_name" -> "Client"
    )

    DSConfigSetupUtils.validateAndGetJDBCConfig(opts) match {
      case Invalid(_) =>
        fail
      case Valid(jdbcConfig) =>
        assert(jdbcConfig.host == "1.1.1.1")
        assert(jdbcConfig.port == 1234)
        assert(jdbcConfig.db == "testdb")
        assert(jdbcConfig.auth.asInstanceOf[KerberosAuth].username == "user")
        assert(jdbcConfig.auth.asInstanceOf[KerberosAuth].kerberosServiceName == "vertica")
        assert(jdbcConfig.auth.asInstanceOf[KerberosAuth].kerberosHostname == "vertica.example.com")
        assert(jdbcConfig.auth.asInstanceOf[KerberosAuth].jaasConfigName == "Client")
    }
  }

  it should "parse the JDBC SSL configuration options" in {
    val opts = Map(
      "host" -> "1.1.1.1",
      "port" -> "1234",
      "db" -> "testdb",
      "user" -> "user",
      "password" -> "password",
      "ssl" -> "true",
      "key_store_path" -> "/.keystore",
      "key_store_password" -> "keystorepass",
      "trust_store_path" -> "/.truststore",
      "trust_store_password" -> "truststorepass"
    )

    DSConfigSetupUtils.validateAndGetJDBCConfig(opts) match {
      case Invalid(errSeq) =>
        fail("The configuration was not valid: \n" +
          errSeq.toList.map(err => err.getUserMessage).mkString("\n"))
      case Valid(jdbcConfig) =>
        val sslConfig = jdbcConfig.sslConfig
        assert(sslConfig.ssl)
        assert(sslConfig.keyStorePath.contains("/.keystore"))
        assert(sslConfig.keyStorePassword.contains("keystorepass"))
        assert(sslConfig.trustStorePath.contains("/.truststore"))
        assert(sslConfig.trustStorePassword.contains("truststorepass"))
    }
  }

  it should "return an error on an invalid SSL parameter" in {
    val opts = Map(
      "host" -> "1.1.1.1",
      "port" -> "1234",
      "db" -> "testdb",
      "user" -> "user",
      "password" -> "password",
      "ssl" -> "blah"
    )

    DSConfigSetupUtils.validateAndGetJDBCConfig(opts) match {
      case Invalid(errSeq) =>
        assert(errSeq.toNonEmptyList.size == 1)
        assert(!errSeq.filter(err => err == SSLFlagParseError()).isEmpty)
      case Valid(jdbcConfig) =>
        fail // should not succeed
    }
  }

  it should "return several configuration errors" in {
    val opts = Map[String, String]()

    DSConfigSetupUtils.validateAndGetJDBCConfig(opts) match {
      case Invalid(errSeq) =>
        assert(errSeq.toNonEmptyList.size == 3)
        assert(!errSeq.filter(err => err == UserMissingError()).isEmpty)
        assert(!errSeq.filter(err => err == DbMissingError()).isEmpty)
        assert(!errSeq.filter(err => err == HostMissingError()).isEmpty)
      case Valid(_) =>
        fail // should not succeed
    }
  }
}
