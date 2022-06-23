package com.vertica.spark.functests.endtoend

import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import com.vertica.spark.datasource.jdbc.VerticaJdbcLayer
import com.vertica.spark.functests.TestUtils
import com.vertica.spark.util.error.{ConnectorException, ErrorList, InternalMapNotSupported, QueryReturnsComplexTypes}
import com.vertica.spark.util.schema.{MetadataKey, ComplexTypeSchemaSupport}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class ComplexTypeTests(readOpts: Map[String, String], writeOpts: Map[String, String], jdbcConfig: JDBCConfig, fileStoreConfig: FileStoreConfig)
  extends EndToEnd(readOpts, writeOpts, jdbcConfig, fileStoreConfig) {

  private val jsonReadOpts = readOpts + ("json" -> "true")

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

  it should "read array[binary] column" in {
    val tableName = "dftest"
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a array[binary])")

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()
    Try {
      df.collect
    } match {
      case Failure(exception) => fail("Expected to succeed", exception)
      case Success(_) =>
        val schema = df.schema.fields
        assert(schema.head.dataType.isInstanceOf[ArrayType])
        assert(schema.head.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[BinaryType])
    }
    stmt.close()
    TestUtils.dropTable(conn, tableName)
  }

  it should "read dataframe with 1D array with scale and precision" in {
    val tableName1 = "dftest_array"
    val n = 1
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a array[numeric(5,2)])")

    val insert = "insert into "+ tableName1 + " values(array[2.5])"
    TestUtils.populateTableBySQL(stmt, insert, n)

    try {
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName1)).load()
      assert(df.count() == 1)
      assert(df.schema.fields(0).dataType.isInstanceOf[ArrayType])
      val dataType = df.schema.fields(0).dataType.asInstanceOf[ArrayType]
      assert(dataType.elementType.isInstanceOf[DecimalType])
      assert(dataType.elementType.asInstanceOf[DecimalType].scale == 2)
      assert(dataType.elementType.asInstanceOf[DecimalType].precision == 5)
      df.rdd.foreach(row => {
        val firstRow = row.getAs[mutable.WrappedArray[java.math.BigDecimal]](0)
        println(firstRow.head.compareTo(new java.math.BigDecimal(2.5)))
      })
    } catch {
      case e: Exception => fail(e)
    } finally {
      stmt.close()
    }
    TestUtils.dropTable(conn, tableName1)
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

  it should "read nested arrays" in {
    val tableName1 = "dftest_array"
    val n = 1
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (col1 int, col2 array[array[int]])")

    val insert = "insert into "+ tableName1 + " values(1, array[array[2]])"
    TestUtils.populateTableBySQL(stmt, insert, n)

    try {
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(jsonReadOpts + ("table" -> tableName1)).load()
      df.show()
      assert(df.count() == 1)
      val col2Schema = df.schema.fields(1)
      assert(col2Schema.dataType.isInstanceOf[ArrayType])
      assert(col2Schema.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[ArrayType])
      assert(col2Schema.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.isInstanceOf[LongType])
    } catch {
      case e: Exception => fail(e)
    } finally {
      stmt.close()
    }
    TestUtils.dropTable(conn, tableName1)
  }

  it should "read struct" in {
    val tableName1 = "dftest_array"
    val n = 1
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (col1 int, col2 row(cat int, dog varchar, mouse array[varchar], shark array[array[int]], row(float)))")

    val insert = "insert into "+ tableName1 + " values(1, row(88, 'hello', array['heelo'], array[array[55]], row(4.2)))"
    TestUtils.populateTableBySQL(stmt, insert, n)

    try {
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(jsonReadOpts + ("table" -> tableName1)).load()
      val rowData = df.collect()(0)
      assert(df.count() == 1)
      // Checking col 1
      assert(rowData.schema.fields(0).dataType.isInstanceOf[LongType])
      assert(rowData.schema.fields(0).name == "col1")
      assert(rowData.getLong(0) == 1)

      val col2Data = rowData.getAs[Row](1)
      val col2Schema = rowData.schema.fields(1)
      assert(col2Schema.dataType.isInstanceOf[StructType])
      assert(col2Schema.name == "col2")
      val struct = col2Schema.dataType.asInstanceOf[StructType]
      assert(struct.fields.length == 5)
      // Struct field 1
      assert(struct.fields(0).dataType.isInstanceOf[LongType])
      assert(struct.fields(0).name == "cat")
      assert(col2Data.getLong(0) == 88)
      // Struct field 2
      assert(struct.fields(1).dataType.isInstanceOf[StringType])
      assert(struct.fields(1).name == "dog")
      assert(col2Data.getString(1) == "hello")
      // Struct field 3
      assert(struct.fields(2).dataType.isInstanceOf[ArrayType])
      assert(struct.fields(2).dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StringType])
      assert(struct.fields(2).name == "mouse")
      val array = col2Data.getAs[mutable.WrappedArray[String]](2)
      assert(array(0) == "heelo")
      // Struct field 4
      assert(struct.fields(3).dataType.isInstanceOf[ArrayType])
      assert(struct.fields(3).name == "shark")
      // Check field 4 is nested array.
      val nestedArray = struct.fields(3).dataType.asInstanceOf[ArrayType]
      assert(nestedArray.elementType.isInstanceOf[ArrayType])
      assert(nestedArray.elementType.asInstanceOf[ArrayType].elementType.isInstanceOf[LongType])
      val rootArray = col2Data.getAs[mutable.WrappedArray[mutable.WrappedArray[Long]]](3)
      val innerArray = rootArray(0)
      assert(innerArray(0) == 55)

      // Check field 5
      assert(struct.fields(4).dataType.isInstanceOf[StructType])
      val innerRow = struct.fields(4).dataType.asInstanceOf[StructType]
      assert(innerRow.fields(0).dataType.isInstanceOf[DoubleType])
      assert(innerRow.fields(0).name == "f0")
      assert(col2Data.getAs[Row](4).getDouble(0) == 4.2)
    } catch {
      case e: Exception => fail(e)
    } finally {
      stmt.close()
    }
    TestUtils.dropTable(conn, tableName1)
  }

  it should "read struct with numeric types" in {
    val tableName1 = "dftest_array"
    val n = 1
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (col1 int, col2 row(numeric(4, 1), numeric(5, 2), array[numeric(6, 3)], array[array[numeric(7, 4)]]))")

    val insert = "insert into "+ tableName1 + " values(1, row(1.2, 2.5, array[3.6], array[array[5.8]]))"
    TestUtils.populateTableBySQL(stmt, insert, n)

    try {
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(jsonReadOpts + ("table" -> tableName1)).load()
      df.show()
      assert(df.count == 1)
      val col2Schema = df.schema.fields(1)
      assert(col2Schema.dataType.isInstanceOf[StructType])
      val struct = col2Schema.dataType.asInstanceOf[StructType]
      assert(struct.fields.length == 4)
      // Struct field 1
      assert(struct.fields(0).dataType.isInstanceOf[DecimalType])
      assert(struct.fields(0).dataType.asInstanceOf[DecimalType].precision == 4)
      assert(struct.fields(0).dataType.asInstanceOf[DecimalType].scale == 1)
      // Struct field 2
      assert(struct.fields(1).dataType.isInstanceOf[DecimalType])
      assert(struct.fields(1).dataType.asInstanceOf[DecimalType].precision == 5)
      assert(struct.fields(1).dataType.asInstanceOf[DecimalType].scale == 2)
      // Struct field 3
      assert(struct.fields(2).dataType.isInstanceOf[ArrayType])
      assert(struct.fields(2).dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[DecimalType])
      assert(struct.fields(2).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[DecimalType].precision == 6)
      assert(struct.fields(2).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[DecimalType].scale == 3)
      // Struct field 4
      assert(struct.fields(3).dataType.isInstanceOf[ArrayType])
      assert(struct.fields(3).dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[ArrayType])
      assert(struct.fields(3).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.isInstanceOf[DecimalType])
      assert(struct.fields(3).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.asInstanceOf[DecimalType].precision == 7)
      assert(struct.fields(3).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.asInstanceOf[DecimalType].scale == 4)
    } catch {
      case e: Exception => fail(e)
    } finally {
      stmt.close()
    }
    TestUtils.dropTable(conn, tableName1)
  }

  it should "read arrays with numeric types" in {
    val tableName1 = "dftest_array"
    val n = 1
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (col1 int, col2 array[numeric(5, 2)], col3 array[array[numeric(6, 2)]])")

    val insert = "insert into "+ tableName1 + " values(1, array[1.5], array[array[2.5]])"
    TestUtils.populateTableBySQL(stmt, insert, n)

    try {
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(jsonReadOpts + ("table" -> tableName1)).load()
      df.show()
      assert(df.count() == 1)

      val col2 = df.schema.fields(1)
      assert(col2.dataType.isInstanceOf[ArrayType])
      assert(col2.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[DecimalType])
      assert(col2.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[DecimalType].precision == 5)
      assert(col2.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[DecimalType].scale == 2)

      val col3 = df.schema.fields(2)
      assert(col3.dataType.isInstanceOf[ArrayType])
      assert(col3.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[ArrayType])
      assert(col3.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.isInstanceOf[DecimalType])
      assert(col3.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.asInstanceOf[DecimalType].precision == 6)
      assert(col3.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.asInstanceOf[DecimalType].scale == 2)
    } catch {
      case e: Exception => fail(e)
    } finally {
      stmt.close()
    }
    TestUtils.dropTable(conn, tableName1)
  }

  it should "read array[row]" in {
    val tableName1 = "dftest_array"
    val n = 1
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (col1 int, col2 array[row(int, int)])")

    val insert = "insert into "+ tableName1 + " values(1, array[row(30, 60)])"
    TestUtils.populateTableBySQL(stmt, insert, n)

    try {
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(jsonReadOpts + ("table" -> tableName1)).load()
      df.show()
      assert(df.count() == 1)
      val col2Schema = df.schema.fields(1)
      assert(col2Schema.dataType.isInstanceOf[ArrayType])
      assert(col2Schema.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType])
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
      assert(verticaId > ComplexTypeSchemaSupport.VERTICA_SET_BASE_ID & verticaId < ComplexTypeSchemaSupport.VERTICA_SET_MAX_ID)
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

  // it should "error on reading complex types" in {
  //   Try {
  //     val tableName = "dftest"
  //     val stmt = conn.createStatement
  //     val n = 3
  //     // Creates a table called dftest with an integer attribute
  //     TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int, b row(int), c array[array[int]])")
  //     val insert = "insert into " + tableName + " values(2, row(2), array[array[10]])"
  //     // Inserts 20 rows of the value '2' into dftest
  //     TestUtils.populateTableBySQL(stmt, insert, n)
  //     // Read dftest into a dataframe
  //     val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource")
  //       .options(readOpts + ("table" -> tableName))
  //       .load()
  //     df.show()
  //   } match {
  //     case Success(_) => fail("Expected error on reading complex types")
  //     case Failure(exp) => exp match {
  //       case ConnectorException(connectorErr) => connectorErr match {
  //         case ComplexTypeReadNotSupported(_,_) => succeed
  //         case err => fail("Unexpected error: " + err.getFullContext)
  //       }
  //       case exp => fail("Unexpected exception", exp)
  //     }
  //   }
  // }

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

  // Currently Row type is not supported
  ignore should "write a table with column type Array[Row]" in {
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
    val tableName = "dftest-map"
    val schema = new StructType(Array(
      StructField("col1", MapType(IntegerType, IntegerType)),
      StructField("col2", IntegerType)
    ))
    val data = Seq(
      Row(
        Map(77 -> 88),
        55)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val mode = SaveMode.Overwrite
    val externalDataPath = fsConfig.address + tableName
    val options = writeOpts + ("table" -> tableName, "create_external_table" -> "true", "staging_fs_url" -> externalDataPath)

    val result = Try {
      df.write.format(VERTICA_SOURCE)
        .options(options)
        .mode(mode)
        .save()

      val jdbcLayer = new VerticaJdbcLayer(jdbcConfig)
      jdbcLayer.configureSession(fsLayer)
      jdbcLayer.query(s"""select col2 from "$tableName";""") match {
        case Left(err) =>
          fail(err.getUnderlyingError.getFullContext)
        case Right(rs) =>
          assert(rs.next)
          assert(rs.getInt(1) == 55)
          assert(!rs.next)
      }
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

  it should "error when using query option that returns complex types" in {
    val tableName1 = "dftest1"
    val stmt = conn.createStatement
    val n = 1
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a int, b array[int], c row(int))")

    val insert = "insert into "+ tableName1 + " values(2, array[4,5], row(7))"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val query = "select * from " + tableName1

    val result = Try{spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("query" -> query)).load().show()}
    result match {
      case Failure(exception) => exception match {
        case ConnectorException(error) =>
          assert(error.isInstanceOf[ErrorList])
          assert(error.asInstanceOf[ErrorList].errors.head.isInstanceOf[ErrorList])
          val errorsFound = error.asInstanceOf[ErrorList].errors.head.getUnderlyingError.asInstanceOf[ErrorList]
          assert(errorsFound.errors.length == 2)
          errorsFound.errors.map(e => assert(e.isInstanceOf[QueryReturnsComplexTypes]))
        case _ => fail("Expected connector exception")
      }
      case Success(_) =>
        fail("Expected to fail")
    }
    TestUtils.dropTable(conn, tableName1)
  }
}
