package com.vertica.spark.functests

import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import com.vertica.spark.util.error.{ComplexTypeReadNotSupported, ComplexTypeWriteNotSupported, ConnectorException}
import com.vertica.spark.util.schema.MetadataKey
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, StructField, StructType}

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

    df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

    val stmt = conn.createStatement()
    val query = s"SELECT to_json($colName) FROM " + tableName
    try {
      val rs1 = stmt.executeQuery(query)
      assert(rs1.next)
      assert(rs1.getString(1).equals("[88,99,111]"))
      val rs2 = stmt.executeQuery(s"select data_type_id from columns where table_name='$tableName' and column_name='$colName';")
      rs2.next
      assert(rs2.getLong(1) == 1506)
    } catch {
      case err: Exception => fail(err)
    } finally {
      stmt.close()
      TestUtils.dropTable(conn, tableName)
    }
  }

  it should "Error on reading 1D array from internal table" in {
    val tableName = "dftest"
    val stmt = conn.createStatement
    val n = 1
    TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a Array[int])")

    val insert = "insert into "+ tableName + " values(array[1,2,3,4,5])"
    TestUtils.populateTableBySQL(stmt, insert, n)

    try{
      spark.read.format("com.vertica.spark.datasource.VerticaSource")
        .options(readOpts + ("table" -> tableName))
        .load()
        .show()
      fail("Expected an exception")
    } catch {
      case throwable: Throwable => throwable match {
        case exp: ConnectorException =>
          assert(exp.error.isInstanceOf[ComplexTypeReadNotSupported])
          val error = exp.error.asInstanceOf[ComplexTypeReadNotSupported]
        case _ => fail(s"Expected ConnectorException. Got ${throwable.toString}")
      }
    }

    TestUtils.dropTable(conn, tableName)
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

    Try {
      df.write.format("com.vertica.spark.datasource.VerticaSource")
        .options(option)
        .mode(mode)
        .save()
      fail()
    } match {
      case Failure(exception) => exception match {
        case ConnectorException(err) => err match {
          case value: ComplexTypeWriteNotSupported =>
            value.colList.foreach(col => {
              assert(schema.exists(_.name.equals(col.name)))
            })
          case _ => fail("Unexpected Connector Error")
        }
        case _ => fail("Expected ConnectorException.", exception)
      }
      case Success(_) => fail("Expected failure.")
    }

    TestUtils.dropTable(conn, tableName)
  }

  it should "read Vertica SET as ARRAY" in {
    val tableName1 = "dftest_array"
    val n = 10
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a SET[int])")
    val insert = "insert into "+ tableName1 + " values(set[0,1,2,3,4,5])"
    TestUtils.populateTableBySQL(stmt, insert, n)

    try{
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName1)).load()

      assert(df.count() == n)
      val arrayCol = df.schema.fields(0)
      assert(arrayCol.dataType.isInstanceOf[ArrayType])
      assert(arrayCol.metadata.getBoolean(MetadataKey.IS_VERTICA_SET))
      val elementDataType = arrayCol.dataType.asInstanceOf[ArrayType]
      assert(elementDataType.elementType.isInstanceOf[LongType])
      df.rdd.foreach(row => {
        assert(row.get(0).isInstanceOf[mutable.WrappedArray[Long]])
        val array = row.getAs[mutable.WrappedArray[Long]](0)
        (0 to 5).foreach(i => {
          assert(array(i) == i)
        })
      }
      )
    }catch {
      case e: Exception => fail(e)
    }finally {
      stmt.close()
      TestUtils.dropTable(conn, tableName1)
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
}
