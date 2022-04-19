package com.vertica.spark.functests.endtoend

import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import com.vertica.spark.functests.TestUtils
import com.vertica.spark.util.error.{ComplexTypeReadNotSupported, ComplexTypeWriteNotSupported, ConnectorException, NativeArrayReadNotSupported}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

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

  it should "error on writing complex type data to Vertica" in {
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
    }
    TestUtils.dropTable(conn, tableName)

    result match {
      case Success(_) => fail("Expected failure")
      case Failure(exception) => exception match {
        case ConnectorException(err) => err match {
          case value: ComplexTypeWriteNotSupported =>
            value.colList.foreach(col => {
              assert(schema.exists(_.name.equals(col.name)))
            })
          case _ => fail("Unexpected Connector Error")
        }
        case e: Throwable => fail("Expected Exception.", e)
      }
    }
  }

  it should "error on reading native arrays from external table in Vertica 10" in {
    val tableName = "dftest"
    val stagingPath = fsConfig.address + tableName

    val data = Seq(Array(1, 5, 3, 4, 7, 2))
    val rdd = spark.sparkContext.parallelize(data)
    val rowRdd = rdd.map(attributes => Row(attributes))
    val schema = StructType(Array(
      StructField("col1", ArrayType(IntegerType, true), true, Metadata.empty),
    ))

    val df = spark.createDataFrame(rowRdd, schema)
    df.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(stagingPath)

    val create = s"create external table $tableName (col1 Array[int]) as copy from 'webhdfs://hdfs:50070/data/dftest/*.parquet' parquet;"
    TestUtils.createTableBySQL(conn, tableName, create)

    val options = readOpts + ("table" -> tableName)
    val result = Try {
      spark.read.format(VERTICA_SOURCE)
        .options(options)
        .load()
        .show()
    }
    TestUtils.dropTable(conn, tableName)

    result match {
      case Failure(exception) => exception match {
        case ConnectorException(error) => error match {
          case NativeArrayReadNotSupported(cols, _) => succeed
          case _ => fail("Unexpected connector error: " + error.getFullContext, exception)
        }
        case _:Throwable => fail(s"Unexpected exception: ${exception.getMessage}", exception)
      }
      case Success(_) => fail("Expected failure")
    }
  }

  it should "error on reading complex types from external table in Vertica 10" in {
    val tableName = "dftest"
    val stagingPath = fsConfig.address + tableName

    val data = Seq(
      Array(Array(1, 5, 3, 4, 7, 2))
    )
    val rdd = spark.sparkContext.parallelize(data)
    val rowRdd = rdd.map(attributes => Row(attributes))
    val schema = StructType(Array(
      StructField("col1", ArrayType(ArrayType(IntegerType, true), true), true, Metadata.empty),
      // StructField("col2", StructType(Array(StructField("f1", StringType))), true, Metadata.empty),
    ))

    val df = spark.createDataFrame(rowRdd, schema)
    df.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(stagingPath)

    val create = s"create external table $tableName (col1 Array[Array[int]]) as copy from 'webhdfs://hdfs:50070/data/dftest/*.parquet' parquet;"
    TestUtils.createTableBySQL(conn, tableName, create)

    val options = readOpts + ("table" -> tableName)
    val result = Try {
      spark.read.format(VERTICA_SOURCE)
        .options(options)
        .load()
        .show()
    }
    TestUtils.dropTable(conn, tableName)

    result match {
      case Failure(exception) => exception match {
        case ConnectorException(error) => error match {
          case ComplexTypeReadNotSupported(_,_) => succeed
          case _ => fail("Unexpected connector error: " + error.getFullContext, exception)
        }
        case _:Throwable => fail(s"Unexpected exception: ${exception.getMessage}", exception)
      }
      case Success(_) => fail("Expected failure")
    }
  }

  it should "read varchar type from Vertica 10 with dbschema specified" in {
    val tableName = "dftest"
    val stmt = conn.createStatement
    val n = 1
    TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a varchar)")

    val insert = "insert into "+ tableName + " values(\'test value\')"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val result = Try{spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()}

    result match {
      case Failure(exception) => fail("Unexpected exception", exception)
      case Success(df: DataFrame) =>
        assert(df.count() == 1)
        df.rdd.foreach(row => assert(row.getAs[String](0) == "test value"))
    }
    TestUtils.dropTable(conn, tableName)
  }
}
