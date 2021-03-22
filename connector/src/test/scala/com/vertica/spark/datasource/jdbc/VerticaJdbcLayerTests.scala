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

package com.vertica.spark.datasource.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import ch.qos.logback.classic.Level
import com.vertica.spark.config.JDBCConfig
import com.vertica.spark.util.error.{ConnectionDownError, ConnectionError}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll

import scala.util.{Failure, Success}

class VerticaJdbcLayerTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {
  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
  }

  private val port = 1234
  private val jdbcConfig: JDBCConfig = JDBCConfig("1.1.1.1", port, "test", "test", "test", Level.ERROR)

  it should "create a connection" in {
    val mockConnection = mock[Connection]
    (mockConnection.isValid _).expects(*).returning(true)
    (mockConnection.setAutoCommit _).expects(*).returning(())
    VerticaJdbcLayer.makeConnection(jdbcConfig, (_: String, _: Properties) => Success(mockConnection)) match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
  }

  it should "fail to create a connection" in {
    val exception = new Exception()
    val result = VerticaJdbcLayer.makeConnection(jdbcConfig, (_: String, _: Properties) => Failure(exception))
    result match {
      case Left(err) => assert(err.getError == ConnectionError(exception))
      case Right(_) => fail("An error was expected, but no error was returned.")
    }
  }

  it should "get a ConnectionDownError" in {
    val mockConnection = mock[Connection]
    (mockConnection.isValid _).expects(*).returning(false)

    val result = VerticaJdbcLayer.makeConnection(jdbcConfig, (_: String, _: Properties) => Success(mockConnection))

    result match {
      case Left(err) => assert(err.getError == ConnectionDownError())
      case Right(_) => fail("An error was expected, but no error was returned.")
    }
  }

  it should "close a connection" in {
    val mockConnection = mock[Connection]
    (mockConnection.isValid _).expects(*).returning(true)
    (mockConnection.setAutoCommit _).expects(*).returning(())
    (mockConnection.isValid _).expects(*).returning(true)
    (mockConnection.close _).expects().returning(())

    val result = for {
      connection <- VerticaJdbcLayer.makeConnection(jdbcConfig, (_: String, _: Properties) => Success(mockConnection))
      jdbcLayer = new VerticaJdbcLayer(connection, jdbcConfig)
      _ <- jdbcLayer.close()
    } yield ()

    result match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
  }

  it should "rollback" in {
    val mockConnection = mock[Connection]
    (mockConnection.isValid _).expects(*).returning(true)
    (mockConnection.setAutoCommit _).expects(*).returning(())
    (mockConnection.isValid _).expects(*).returning(true)
    (mockConnection.rollback _).expects().returning(())

    val result = for {
      connection <- VerticaJdbcLayer.makeConnection(jdbcConfig, (_: String, _: Properties) => Success(mockConnection))
      jdbcLayer = new VerticaJdbcLayer(connection, jdbcConfig)
      _ <- jdbcLayer.rollback()
    } yield ()

    result match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
  }

  it should "commit" in {
    val mockConnection = mock[Connection]
    (mockConnection.isValid _).expects(*).returning(true)
    (mockConnection.setAutoCommit _).expects(*).returning(())
    (mockConnection.isValid _).expects(*).returning(true)
    (mockConnection.commit _).expects().returning(())

    val result = for {
      connection <- VerticaJdbcLayer.makeConnection(jdbcConfig, (_: String, _: Properties) => Success(mockConnection))
      jdbcLayer = new VerticaJdbcLayer(connection, jdbcConfig)
      _ <- jdbcLayer.commit()
    } yield ()

    result match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
  }

  it should "query" in {
    val mockConnection = mock[Connection]
    val mockPreparedStatement = mock[PreparedStatement]
    (mockConnection.isValid _).expects(*).returning(true)
    (mockConnection.setAutoCommit _).expects(*).returning(())
    (mockConnection.isValid _).expects(*).returning(true)
    (mockConnection.prepareStatement(_: String)).expects("").returning(mockPreparedStatement)
    (mockPreparedStatement.executeQuery _).expects().returning(mock[ResultSet])

    val result = for {
      connection <- VerticaJdbcLayer.makeConnection(jdbcConfig, (_: String, _: Properties) => Success(mockConnection))
      jdbcLayer = new VerticaJdbcLayer(connection, jdbcConfig)
      rs <- jdbcLayer.query("")
    } yield rs

    result match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
  }
}
