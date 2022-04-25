package com.vertica.spark.functests.endtoend

import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import com.vertica.spark.functests.TestUtils
import com.vertica.spark.util.error.{ComplexTypeReadNotSupported, ConnectorException, InternalMapNotSupported}
import com.vertica.spark.util.schema.{MetadataKey, SchemaTools}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class ComplexTypeTests(readOpts: Map[String, String], writeOpts: Map[String, String], jdbcConfig: JDBCConfig, fileStoreConfig: FileStoreConfig)
  extends EndToEnd(readOpts, writeOpts, jdbcConfig, fileStoreConfig) {

  it should "read dataframe with 1D array" in {
    val tableName1 = "dftest_array"
    val n = 1
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a array[int])")

    val insert = "insert into "+ tableName1 + " values(array[2])"
    TestUtils.populateTableBySQL(stmt, insert, n)

    try {
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName1)).load()
      assert(df.count() == 1)
      assert(df.schema.fields(0).dataType.isInstanceOf[ArrayType])
      val dataType = df.schema.fields(0).dataType.asInstanceOf[ArrayType]
      assert(dataType.elementType.isInstanceOf[LongType])
      df.rdd.foreach(row => assert(row.getAs[mutable.WrappedArray[Long]](0)(0) == 2))
    } catch {
      case e: Exception => fail(e)
    } finally {
      stmt.close()
    }
    TestUtils.dropTable(conn, tableName1)
  }

  it should "write 1D array" in {
    val tableName = "native_array_write_test"
    val colName = "col1"
    val schema = new StructType(Array(StructField(colName, ArrayType(IntegerType))))

    val data = Seq(Row(Array(88,99,111)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    println(df.toString())
    val mode = SaveMode.Overwrite

    df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

    val stmt = conn.createStatement()
    val query = s"SELECT $colName FROM " + tableName
    try {
      val rs = stmt.executeQuery(query)
      assert (rs.next)
      val array = rs.getArray(colName).getArray.asInstanceOf[Array[AnyRef]]
      assert(array(0) == 88L)
      assert(array(1) == 99L)
      assert(array(2) == 111L)
      val columnRs = stmt.executeQuery(s"select data_type_length from columns where table_name='$tableName' and column_name='$colName'")
      assert(columnRs.next)
      assert(columnRs.getLong("data_type_length") == 65000L)
    }
    catch{
      case err : Exception => fail(err)
    }
    finally {
      stmt.close()
      TestUtils.dropTable(conn, tableName)
    }
  }

  it should "write 1D bounded array" in {
    val tableName = "native_array_write_test"
    val colName = "col1"
    val schema = new StructType(Array(StructField(colName, ArrayType(IntegerType))))

    val data = Seq(Row(Array(88,99,111)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    println(df.toString())
    val mode = SaveMode.Overwrite
    df.write.format("com.vertica.spark.datasource.VerticaSource")
      .options(writeOpts + ("table" -> tableName, "array_length" -> "10"))
      .mode(mode).save()

    val stmt = conn.createStatement()
    val query = s"SELECT $colName FROM " + tableName
    try {
      val rs = stmt.executeQuery(query)
      assert (rs.next)
      val array = rs.getArray(colName).getArray.asInstanceOf[Array[AnyRef]]
      assert(array(0) == 88L)
      assert(array(1) == 99L)
      assert(array(2) == 111L)
      val columnRs = stmt.executeQuery(s"select data_type_length from columns where table_name='$tableName' and column_name='$colName'")
      assert(columnRs.next)
      assert(columnRs.getLong("data_type_length") == 8 * 10)
    }
    catch{
      case err : Exception => fail(err)
    }
    finally {
      stmt.close()
      TestUtils.dropTable(conn, tableName)
    }
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

  it should "write SET to Vertica" in {
    val tableName = "dftest"
    // Ensure that we are create the table from scratch
    TestUtils.dropTable(conn, tableName)

    val colName = "col1"
    val metadata = new MetadataBuilder().putBoolean(MetadataKey.IS_VERTICA_SET, true).build
    val schema = new StructType(Array(StructField(colName, ArrayType(IntegerType), metadata = metadata)))
    val data = Seq(Row(Array(88,99,111)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    println(df.toString())
    val mode = SaveMode.Overwrite

    df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

    val stmt = conn.createStatement()
    val query = s"SELECT $colName FROM " + tableName
    try {
      val rs = stmt.executeQuery(query)
      assert (rs.next)
      val array = rs.getArray(colName).getArray.asInstanceOf[Array[AnyRef]]
      assert(array(0) == 88L)
      assert(array(1) == 99L)
      assert(array(2) == 111L)
      val columnRs = stmt.executeQuery(s"select data_type_id from columns where table_name='$tableName' and column_name='$colName'")
      assert(columnRs.next)
      val verticaId = columnRs.getLong("data_type_id")
      assert(verticaId > SchemaTools.VERTICA_SET_BASE_ID & verticaId < SchemaTools.VERTICA_SET_MAX_ID)
    } catch {
      case err : Exception => fail(err)
    }
    finally {
      stmt.close()
    }

    TestUtils.dropTable(conn, tableName)
  }

  it should "write nested arrays array" in {
    val tableName = "nested_array_write_test"
    val colName = "col1"
    val schema = new StructType(Array(
      StructField("x", IntegerType),
      StructField(colName, ArrayType(ArrayType(IntegerType)))))

    val data = Seq(Row(1, Array(Array(88, 99, 111))))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    println(df.toString())
    val mode = SaveMode.Overwrite

    df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

    val stmt = conn.createStatement()
    val query = s"SELECT $colName FROM " + tableName
    try {
      val rs = stmt.executeQuery(query)
      assert(rs.next)
      val array = rs.getArray(colName).getArray.asInstanceOf[Array[AnyRef]]
      val nestedArray = array(0).asInstanceOf[Array[AnyRef]]
      assert(nestedArray(0) == 88L)
      assert(nestedArray(1) == 99L)
      assert(nestedArray(2) == 111L)
    }
    catch {
      case err: Exception => fail(err)
    }
    finally {
      stmt.close()
      TestUtils.dropTable(conn, tableName)
    }
  }

  it should "error on reading complex types" in {
    Try {
      val tableName = "dftest"
      val stmt = conn.createStatement
      val n = 3
      // Creates a table called dftest with an integer attribute
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int, b row(int), c array[array[int]])")
      val insert = "insert into " + tableName + " values(2, row(2), array[array[10]])"
      // Inserts 20 rows of the value '2' into dftest
      TestUtils.populateTableBySQL(stmt, insert, n)
      // Read dftest into a dataframe
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource")
        .options(readOpts + ("table" -> tableName))
        .load()
      df.show()
    } match {
      case Success(_) => fail("Expected error on reading complex types")
      case Failure(exp) => exp match {
        case ConnectorException(connectorErr) => connectorErr match {
          case ComplexTypeReadNotSupported(_,_) => succeed
          case err => fail("Unexpected error: " + err.getFullContext)
        }
        case exp => fail("Unexpected exception", exp)
      }
    }
  }

  it should "write table with a struct of primitives" in {
    val tableName = "dftest"
    val colName = "col1"
    val schema = new StructType(Array(
      StructField("required", IntegerType),
      StructField(colName, StructType(Array(
        StructField("field1", IntegerType, false, Metadata.empty)
      )))))

    val data = Seq(Row(1,Row(77)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    println(df.toString())
    val mode = SaveMode.Overwrite

    df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

    val stmt = conn.createStatement()
    val query = s"SELECT $colName FROM " + tableName
    try {
      val rs = stmt.executeQuery(query)
      assert(rs.next)
      val struct = rs.getObject(colName).asInstanceOf[java.sql.Struct]
      val fields = struct.getAttributes()
      assert(fields.length == 1)
      println(fields(0).isInstanceOf[Long])
      println(fields(0) == 77)
    }
    catch{
      case err : Exception => fail(err)
    }
    finally {
      stmt.close()
    }

    TestUtils.dropTable(conn, tableName)
  }

  it should "write table with a struct of complex type" in {
    val tableName = "dftest"
    val colName = "col1"
    val schema = new StructType(Array(
      StructField("required", IntegerType),
      StructField(colName, StructType(Array(
        StructField("field1", ArrayType(ArrayType(IntegerType))),
        StructField("field2", StructType(Array(StructField("field3",IntegerType)))),
      )))))

    val data = Seq(
      Row(1, Row(
        Array(Array(77)),
        Row(88)
      )))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    println(df.toString())
    val mode = SaveMode.Overwrite

    df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

    val stmt = conn.createStatement()
    val query = s"SELECT $colName FROM " + tableName
    try {
      val rs = stmt.executeQuery(query)
      assert(rs.next)
      val struct = rs.getObject(colName).asInstanceOf[java.sql.Struct]
      val fields = struct.getAttributes
      assert(fields.length == 2)
      val field1 = fields(0).asInstanceOf[Array[AnyRef]]
      val nestedArrElement = field1(0).asInstanceOf[Array[AnyRef]](0)
      val field2 = fields(1).asInstanceOf[java.sql.Struct]
      assert(field2.getAttributes.length == 1)
      val field3 = field2.getAttributes()(0)
      assert(nestedArrElement == 77)
      assert(field3 == 88)
    }
    catch{
      case err : Exception => fail(err)
    }
    finally {
      stmt.close()
    }

    TestUtils.dropTable(conn, tableName)
  }

  it should "write a table with column type Array[Row]" in {
    val tableName = "dftest"
    val colName = "col1"
    val arrayRow = StructField(colName, ArrayType(
      StructType(Array(
        StructField("key", StringType),
        StructField("value", IntegerType),
      ))
    ))

    val schema = new StructType(Array(
      StructField("required", IntegerType),
      arrayRow
    ))

    val data = Seq(Row(1, Array(Row("key1", 77))))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    println(df.toString())
    val mode = SaveMode.Overwrite

    df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

    val stmt = conn.createStatement()
    val query = s"SELECT $colName FROM " + tableName
    try {
      val rs = stmt.executeQuery(query)
      assert (rs.next)
      val array = rs.getArray(colName).getArray.asInstanceOf[Array[AnyRef]]
      val fields = array(0).asInstanceOf[java.sql.Struct].getAttributes
      assert(fields.length == 2)
      assert(fields(0) == "key1")
      assert(fields(1) == 77)
    }
    catch{
      case err : Exception => fail(err)
    }
    finally {
      stmt.close()
    }

    TestUtils.dropTable(conn, tableName)
  }

  it should "write external table with Vertica map" in {
    val tableName = "dftest"
    val schema = new StructType(Array(
      StructField("col1", MapType(IntegerType, IntegerType)),
      StructField("col2", IntegerType)
    ))
    val data = Seq(Row(Map()+(77 -> 88), 55))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val mode = SaveMode.Overwrite
    val options = writeOpts + ("table" -> tableName, "create_external_table" -> "true", "staging_fs_url" -> (fsConfig.address +"dftest"))

    val result = Try {
      df.write.format(VERTICA_SOURCE)
        .options(options)
        .mode(mode)
        .save()

      val rs = conn.createStatement().executeQuery("select \"col2\" from dftest;")
      rs.next()
      assert(rs.getInt(1) == 55)
    }

    TestUtils.dropTable(conn, tableName)
    // Since we are writing external table, data will persist
    fsLayer.removeDir(fsConfig.address)
    fsLayer.createDir(fsConfig.address, "777")

    result match {
      case Success(_) => succeed
      case Failure(exp) => fail("Expected to succeed", exp)
    }
  }

  it should "error on writing map to internal Vertica table" in {
    val tableName = "dftest"
    val schema = new StructType(Array(StructField("col1", MapType(IntegerType, IntegerType))))
    val data = Seq(Row(Map() + (77 -> 88)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
    val mode = SaveMode.Overwrite

    val result = Try {
      df.write.format("com.vertica.spark.datasource.VerticaSource")
        .options(writeOpts + ("table" -> tableName))
        .mode(mode)
        .save()
    }
    TestUtils.dropTable(conn, tableName)

    result match {
      case Success(_) => fail("Expected to fail")
      case Failure(exp) => exp match {
        case ConnectorException(err) => err match {
          case InternalMapNotSupported() => succeed
          case _ => fail("Unexpected error " + err.getFullContext)
        }
        case _ => fail("Unexpected exception", exp)
      }
    }
  }
}
