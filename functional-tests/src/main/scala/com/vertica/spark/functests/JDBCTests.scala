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

package com.vertica.spark.functests

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import com.vertica.spark.datasource.jdbc._
import com.vertica.spark.config.{BasicJdbcAuth, JDBCConfig, JDBCTLSConfig}
import com.vertica.spark.datasource.core.Disable
import com.vertica.spark.util.error.{ConnectionSqlError, DataError, SyntaxError}
import org.apache.spark.sql.SparkSession

/**
  * Tests basic functionality of the VerticaJdbcLayer
  *
  * Should ensure that the component correctly passes on queries / other statements to vertica and correctly returns results. It should also confirm that error handling works as expected.
  */
class JDBCTests(val jdbcCfg: JDBCConfig) extends AnyFlatSpec with BeforeAndAfterAll with BeforeAndAfterEach {
  var jdbcLayer: JdbcLayerInterface = _

  val tablename = "test_table"

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
    .getOrCreate()

  override def beforeAll(): Unit = {
    jdbcLayer = new VerticaJdbcLayer(jdbcCfg)
  }

  override def beforeEach(): Unit = {
    jdbcLayer.execute("DROP TABLE IF EXISTS " + tablename + ";")
  }

  override def afterEach(): Unit = {
    jdbcLayer.execute("DROP TABLE IF EXISTS " + tablename + ";")
  }

  it should "Create a table" in {
    jdbcLayer.execute("CREATE TABLE " + tablename + "(vendor_key integer, vendor_name varchar(64));")

    try {
      jdbcLayer.query("SELECT * FROM " + tablename + " WHERE 1=0;")
    }
    catch {
      case _: Throwable => fail // There should be no error loading the created table
    }
  }

  it should "Insert integer data to table and load" in {
    val result = for {
      _ <- jdbcLayer.execute("CREATE TABLE " + tablename + "(vendor_key integer);")
      _ <- jdbcLayer.execute("INSERT INTO " + tablename + " VALUES(123);")
      rs <- jdbcLayer.query("SELECT * FROM " + tablename + ";")
    } yield rs


    result match {
      case Right(rs) =>
        assert(rs.next())
        assert(rs.getInt(1) == 123)
      case Left(err) =>
        fail(err.getFullContext)
    }
  }

  it should "Insert integer data with param" in {
    jdbcLayer.execute("CREATE TABLE " + tablename + "(vendor_key integer);")

    jdbcLayer.execute("INSERT INTO " + tablename + " VALUES(?);", Seq(new JdbcLayerIntParam(123)))

    jdbcLayer.query("SELECT * FROM " + tablename + ";") match {
      case Right(rs) =>
        assert(rs.next())
        assert(rs.getInt(1) == 123)
      case Left(err) =>
        fail(err.getFullContext)
    }
  }

  it should "Insert string data to table and load" in {
    jdbcLayer.execute("CREATE TABLE " + tablename + "(name varchar(64));")

    jdbcLayer.execute("INSERT INTO " + tablename + " VALUES('abc123');")

    jdbcLayer.query("SELECT * FROM " + tablename + ";") match {
      case Right(rs) =>
        assert(rs.next())
        assert(rs.getString(1) == "abc123")
      case Left(err) =>
        fail(err.getFullContext)
    }
  }

  it should "Insert string data to table with param" in {
    jdbcLayer.execute("CREATE TABLE " + tablename + "(name varchar(64));")

    jdbcLayer.execute("INSERT INTO " + tablename + " VALUES(?);", Seq(new JdbcLayerStringParam("abc123")))

    jdbcLayer.query("SELECT * FROM " + tablename + ";") match {
      case Right(rs) => {
        assert(rs.next())
        assert(rs.getString(1) == "abc123")
      }
      case Left(err) => {
        fail(err.getFullContext)
      }
    }
  }

  it should "Insert mixed data to table and load" in {
    jdbcLayer.execute("CREATE TABLE " + tablename + "(name varchar(64), id integer);")

    jdbcLayer.execute("INSERT INTO " + tablename + " VALUES('abc123', 5);")

    jdbcLayer.query("SELECT * FROM " + tablename + ";") match {
      case Right(rs) => {
        assert(rs.next())
        assert(rs.getString(1) == "abc123")
        assert(rs.getInt(2) == 5)
      }
      case Left(err) => {
        fail(err.getFullContext)
      }
    }
  }

  it should "Fail to load results from a table that doesn't exist" in {
    jdbcLayer.query("SELECT * FROM " + tablename + ";") match {
      case Right(rs) => {
        assert(false)
      }
      case Left(err) => {
        println(err.getError.getFullContext)
        assert(err.getError match {
          case SyntaxError(_) => true
          case _ => false
        })
      }
    }
  }


  it should "Fail to insert results with wrong datatype" in {
    jdbcLayer.execute("CREATE TABLE " + tablename + "(name integer);")

    jdbcLayer.execute("INSERT INTO " + tablename + " VALUES('abc123');") match {
      case Right(u) => assert(false) // should not succeed
      case Left(err) => {
        println(err.getError.getFullContext)
        assert(err.getError match {
          case DataError(_) => true
          case _ => false
        })
      }
    }
  }

  it should "Fail to create a table with bad syntax" in {
    jdbcLayer.execute("CREATE TABLE " + tablename + ";") match {
      case Right(u) => assert(false) // should not succeed
      case Left(err) => {
        assert(err.getError match {
          case _: SyntaxError => true
          case _ => false
        })
      }
    }
  }

  it should "Fail to connect to the wrong database" in {
    val tlsConfig = JDBCTLSConfig(tlsMode = Disable, None, None, None, None)
    val badJdbcLayer = new VerticaJdbcLayer(JDBCConfig(host = jdbcCfg.host, port = jdbcCfg.port, db = jdbcCfg.db + "-doesnotexist123asdf", BasicJdbcAuth(username = "test", password = "test"), tlsConfig))

    badJdbcLayer.execute("CREATE TABLE " + tablename + "(name integer);") match {
      case Right(u) => assert(false) // should not succeed
      case Left(err) => {
        println(err.getFullContext)
        assert(err.getError match {
          case _: ConnectionSqlError => true
          case _ => false
        })
      }
    }
  }

  it should "get the client label" in {
    val result = jdbcLayer.query("SELECT GET_CLIENT_LABEL();")

    result match {
      case Right(rs) =>
        assert(rs.next())
        val label = rs.getString(1)
        assert(label.contains("vspark-vs2.0.0-p-sp3.0.0"))
      case Left(err) =>
        fail(err.getFullContext)
    }
  }
}
