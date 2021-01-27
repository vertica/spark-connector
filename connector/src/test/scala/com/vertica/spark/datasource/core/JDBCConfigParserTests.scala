package com.vertica.spark.datasource.core

import cats.data.Validated.{Invalid, Valid}
import cats.implicits._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import ch.qos.logback.classic.Level
import org.scalamock.scalatest.MockFactory
import com.vertica.spark.util.error.ConnectorErrorType._


class JDBCConfigParserTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  it should "parse the JDBC config" in {
    val opts = Map(
                   "host" -> "1.1.1.1",
                   "port" -> "1234",
                   "db" -> "testdb",
                   "user" -> "user",
                   "password" -> "password"
    )

    val logLevel : Level = Level.ERROR

    DSConfigSetupUtils.validateAndGetJDBCConfig(opts) match {
      case Invalid(_) =>
        fail
      case Valid(jdbcConfig) =>
        assert(jdbcConfig.host == "1.1.1.1")
        assert(jdbcConfig.port == 1234)
        assert(jdbcConfig.db == "testdb")
        assert(jdbcConfig.username == "user")
        assert(jdbcConfig.password == "password")
        println(jdbcConfig.logLevel)
        assert(jdbcConfig.logLevel == logLevel)
    }
  }

  it should "return several configuration errors" in {
    val opts = Map(
                   "host" -> "1.1.1.1"
    )

    DSConfigSetupUtils.validateAndGetJDBCConfig(opts) match {
      case Invalid(errSeq) =>
        assert(errSeq.toNonEmptyList.size == 3)
        assert(!errSeq.filter(err => err.err == UserMissingError).isEmpty)
        assert(!errSeq.filter(err => err.err == PasswordMissingError).isEmpty)
        assert(!errSeq.filter(err => err.err == DbMissingError).isEmpty)
      case Valid(_) =>
        fail // should not succeed
    }
  }

  it should "return all possible configuration errors" in {
    val opts = Map[String, String]()

    DSConfigSetupUtils.validateAndGetJDBCConfig(opts) match {
      case Invalid(errSeq) =>
        assert(errSeq.toNonEmptyList.size == 4)
      case Valid(_) =>
        fail // should not succeed
    }
  }
}
