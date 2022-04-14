package com.vertica.spark.functests.endtoend

import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import com.vertica.spark.functests.TestUtils
import com.vertica.spark.util.error.{ComplexTypeReadNotSupported, ComplexTypeWriteNotSupported, ConnectorException, NativeArrayReadNotSupported}
import com.vertica.spark.util.schema.MetadataKey
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.Assertion

import scala.collection.mutable
import scala.util.{Failure, Success, Try}


class ComplexTypeTestsV10(readOpts: Map[String, String], writeOpts: Map[String, String], jdbcConfig: JDBCConfig, fileStoreConfig: FileStoreConfig)
  extends EndToEnd(readOpts, writeOpts, jdbcConfig, fileStoreConfig) {

  it should "write 1D array to internal table" in {
    val tableName = "dftest"
    val colName = "col1"
    val schema = new StructType(Array(StructField(colName, ArrayType(IntegerType))))

    val data = Seq(Row(Array(88, 99, 111)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    println(df.toString())
    val mode = SaveMode.Overwrite

    val stmt = conn.createStatement()
    val result = Try {
      df.write.format("com.vertica.spark.datasource.VerticaSource")
        .options(writeOpts + ("table" -> tableName)).mode(mode).save()

      val query = s"SELECT to_json($colName) FROM " + tableName

      val rs1 = stmt.executeQuery(query)
      assert(rs1.next)
      assert(rs1.getString(1).equals("[88,99,111]"))
      val rs2 = stmt.executeQuery(s"select data_type_id from columns where table_name='$tableName' and column_name='$colName';")
      rs2.next
      assert(rs2.getLong(1) == 1506)
    }
    stmt.close()
    TestUtils.dropTable(conn, tableName)

    failIfError(result)
  }

  it should "Error on reading 1D array from internal table" in {
    val tableName = "dftest"
    val stmt = conn.createStatement
    val n = 1
    TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a Array[int])")

    val insert = "insert into "+ tableName + " values(array[1,2,3,4,5])"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val result = Try {
      spark.read.format("com.vertica.spark.datasource.VerticaSource")
        .options(readOpts + ("table" -> tableName))
        .load()
        .show()
    }

    TestUtils.dropTable(conn, tableName)

    result match {
      case Success(_) => fail("Expected to fail")
      case Failure(exception) => exception match {
        case ConnectorException(error) => assert(error.isInstanceOf[NativeArrayReadNotSupported])
        case e: Throwable => fail("Unexpected exception", e)
      }
    }
  }

  it should "error on writing complex data to Vertica" in {
    val tableName = "dftest"
    val schema = new StructType(Array(
      StructField("ss", IntegerType),
      StructField("col1", ArrayType(ArrayType(IntegerType))),
      StructField("col3", StructType(Array(StructField("",IntegerType)))),
    ))

    val data = Seq(Row())
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val mode = SaveMode.Overwrite
    val option = writeOpts + ("table" -> tableName)

    val result = Try {
      df.write.format("com.vertica.spark.datasource.VerticaSource")
        .options(option)
        .mode(mode)
        .save()
      fail()
    }
    TestUtils.dropTable(conn, tableName)

    result match {
      case Success(_) => fail("Expected failure")
      case Failure(exception) => exception match {
        case e: Throwable => fail("Expected ConnectorException.", e)
        case ConnectorException(err) => err match {
          case value: ComplexTypeWriteNotSupported =>
            value.colList.foreach(col => {
              assert(schema.exists(_.name.equals(col.name)))
            })
          case _ => fail("Unexpected Connector Error")
        }
      }
    }
  }

  it should "Error on reading complex types from Vertica 10" in {
    val tableName = "dftest"
    val stmt = conn.createStatement
    val n = 1
    TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int)")

    val insert = "insert into "+ tableName + " values(2)"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()

    assert(df.count() == 1)
    df.rdd.foreach(row => assert(row.getAs[Long](0) == 2))
    TestUtils.dropTable(conn, tableName)
  }

  it should "read varchar type from Vertica 10 with dbschema specified" in {
    val tableName = "dftest"
    val stmt = conn.createStatement
    val n = 1
    TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a varchar)")

    val insert = "insert into "+ tableName + " values(\"Test values\")"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()

    assert(df.count() == 1)
    df.rdd.foreach(row => assert(row.getAs[String](0) == "a"))
    TestUtils.dropTable(conn, tableName)
  }
}
