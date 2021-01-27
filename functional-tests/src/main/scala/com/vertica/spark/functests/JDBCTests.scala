package com.vertica.spark.functests

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import com.vertica.spark.util.error.JdbcErrorType._

import com.vertica.spark.datasource.jdbc._
import com.vertica.spark.config.JDBCConfig

/**
  * Tests basic functionality of the VerticaJdbcLayer
  *
  * Should ensure that the component correctly passes on queries / other statements to vertica and correctly returns results. It should also confirm that error handling works as expected.
  */
class JDBCTests(val jdbcCfg: JDBCConfig) extends AnyFlatSpec with BeforeAndAfterAll with BeforeAndAfterEach {
  var jdbcLayer : JdbcLayerInterface = _

  val tablename = "test_table"

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

    try{
      jdbcLayer.query("SELECT * FROM " + tablename + " WHERE 1=0;")
    }
    catch {
      case _ : Throwable => fail // There should be no error loading the created table
    }
  }

  it should "Insert integer data to table and load" in {
    jdbcLayer.execute("CREATE TABLE " + tablename + "(vendor_key integer);")

    jdbcLayer.execute("INSERT INTO " + tablename + " VALUES(123);")

    jdbcLayer.query("SELECT * FROM " + tablename + ";") match {
      case Right(rs) =>
        assert(rs.next())
        assert(rs.getInt(1) == 123)
      case Left(err) =>
        fail
    }
  }

  it should "Insert string data to table and load" in {
    jdbcLayer.execute("CREATE TABLE " + tablename + "(name varchar(64));")

    jdbcLayer.execute("INSERT INTO " + tablename + " VALUES('abc123');")

    jdbcLayer.query("SELECT * FROM " + tablename + ";") match {
      case Right(rs) => {
        assert(rs.next())
        assert(rs.getString(1) == "abc123")
      }
      case Left(err) => {
        assert(false)
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
        assert(false)
      }
    }
  }

  it should "Fail to load results from a table that doesn't exist" in {
    jdbcLayer.query("SELECT * FROM " + tablename + ";") match {
      case Right(rs) => {
        assert(false)
      }
      case Left(err) => {
        assert(err.err == SyntaxError)
      }
    }
  }


  it should "Fail to insert results with wrong datatype" in {
    jdbcLayer.execute("CREATE TABLE " + tablename + "(name integer);")

    jdbcLayer.execute("INSERT INTO " + tablename + " VALUES('abc123');") match {
      case Right(u) => assert(false) // should not succeed
      case Left(err) => {
        assert(err.err == DataTypeError)
      }
    }
  }

  it should "Fail to create a table with bad syntax" in {
    jdbcLayer.execute("CREATE TABLE " + tablename + ";") match {
      case Right(u) => assert(false) // should not succeed
      case Left(err) => {
        assert(err.err == SyntaxError)
      }
    }
  }

  it should "Fail to connect to the wrong database" in {
    val badJdbcLayer = new VerticaJdbcLayer(JDBCConfig(host = jdbcCfg.host, port = jdbcCfg.port, db = jdbcCfg.db+"-doesnotexist123asdf", username = jdbcCfg.username, password = jdbcCfg.password, logLevel=jdbcCfg.logLevel))

    badJdbcLayer.execute("CREATE TABLE " + tablename + "(name integer);") match {
      case Right(u) => assert(false) // should not succeed
      case Left(err) => {
        assert(err.err == ConnectionError)
      }
    }
  }


}
