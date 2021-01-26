package com.vertica.spark.functests

import java.sql.Connection

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class EndToEndTests(readOpts: Map[String, String], tablename: String, tablename20: String) extends AnyFlatSpec with BeforeAndAfterAll {
  val conn: Connection = TestUtils.getJDBCConnection(readOpts("host"), db = readOpts("db"), user = readOpts("user"), password = readOpts("password"))

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()
    conn.close()
  }

  it should "read data from Vertica" in {
    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tablename)).load()

    assert(df.count() == 1)
    df.rdd.foreach(row => assert(row.getAs[Long](0) == 2))
  }

  it should "read 20 rows of data from Vertica" in {
    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tablename20)).load()

    assert(df.count() == 20)
    df.rdd.foreach(row => assert(row.getAs[Long](0) == 2))

  }

  it should "support data frame schema" in {

    //val tableName1 = "dftest1"
    //val stmt = conn.createStatement
    //createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a int, b float)")

    //val insert = "insert into "+ tableName1 + " values(1, 2.2)"
    //populateTableBySQL(stmt, tableName1, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> "dftest1")).load()

    val schema = df.schema
    val sc = StructType(Array(StructField("a",LongType,nullable = true), StructField("b",DoubleType,nullable = true)))

    assert(schema.toString equals sc.toString)
  }

  it should "support data frame projection" in {
    val tableName1 = "dftest1"
    val stmt = conn.createStatement
    val n = 3
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a int, b float)")
    val insert = "insert into "+ tableName1 + " values(1, 2.2)"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    val filtered = df.select(df("a"))
    val count = filtered.count()

    assert(count == n)
    assert(filtered.columns.mkString equals Array("a").mkString)
  }

  it should "support data frame filter" in {

    val tableName1 = "dftest1"

    val stmt = conn.createStatement
    val n = 3
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a int, b float)")
    val insert = "insert into "+ tableName1 + " values(1, 2.2)"
    TestUtils.populateTableBySQL(stmt, insert, n)
    val insert2 = "insert into "+ tableName1 + " values(3, 4.4)"
    TestUtils.populateTableBySQL(stmt, insert2, n)
    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()
    val filtered = df.filter(df("a")> 2).where(df("b") > 3.3)
    val count = filtered.count()

    assert(count == n)
  }

  it should "load data from Vertica table that is [SEGMENTED] on [ALL] nodes" in {
    val tableName1 = "dftest1"

    val n = 40
    TestUtils.createTable(conn, tableName1, numOfRows = n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    df.cache()
    df.show()
    println("schema =" + df.schema)
    val c = df.count()
    println("count = " + c)

    assert(c == n)
  }

  it should "load data from Vertica table that is [UNSEGMENTED] on [ALL] nodes" in {
    val tableName1 = "dftest1"

    val n = 40
    TestUtils.createTable(conn, tableName1, isSegmented = false)
    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    df.cache()
    df.show()
    println("schema =" + df.schema)
    val c = df.count()
    println("count = " + c)

    assert(c == n)
  }

  /*
  it should "load data from Vertica table that is [SEGMENTED] on [SOME] nodes for [arbitrary partition number]" in {
    val tableName1 = "t1"

    val n = 40
    val nodes = getNodeNames(conn)
    createTablePartialNodes(conn, tableName1, true, numOfRows = n, nodes.splitAt(3)._1)

    for (p <- 1 until numSparkPartitions) {
      info("Number of Partition : " + p)
      val r = TestUtils.doCount(spark, readOpts + ("tablename" -> tableName1))
      assert(r == n)
    }
  }

  it should "load data from Vertica table that is [SEGMENTED] on [Some] nodes" in {
    val tableName1 = "dftest1"

    val n = 40
    val nodes = getNodeNames(conn)
    createTablePartialNodes(conn, tableName1, true, numOfRows = n, nodes.splitAt(3)._1)
    val r = TestUtils.doCount(spark, readOpts + ("tablename" -> tableName1))

    assert(r == n)
  }

  it should "load data from Vertica table that is [UNSEGMENTED] on [Some] nodes" in {
    val tableName1 = "dftest1"

    val n = 10
    val nodes = getNodeNames(conn)
    createTablePartialNodes(conn, tableName1, true, numOfRows = n, nodes.splitAt(3)._1)
    val r = TestUtils.doCount(spark, readOpts + ("tablename" -> tableName1))

    assert(r == n)
  }

  it should "load data from Vertica table that is [UNSEGMENTED] on [One] nodes for [arbitrary partition number]" in {
    val tableName1 = "t1"

    val n = 40
    val nodes = getNodeNames(conn)
    val stmt = conn.createStatement()
    stmt.execute("SELECT MARK_DESIGN_KSAFE(0);")
    createTablePartialNodes(conn, tableName1, false, numOfRows = n, nodes.splitAt(3)._1)

    for (p <- 1 until numSparkPartitions) {
      info("Number of Partition : " + p)
      val r = TestUtils.doCount(spark, readOpts + ("tablename" -> tableName1))
      assert(r == n)
    }
    stmt.execute("drop table " + tableName1)
    stmt.execute("SELECT MARK_DESIGN_KSAFE(1);")
  }
   */

  it should "load data from Vertica for [all Binary data types] of Vertica" in {
    val tableName1 = "t1"

    val n = 40
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a binary, b varbinary, c long varbinary, d bytea, e raw)")
    val insert = "insert into "+ tableName1 + " values(hex_to_binary('0xff'), HEX_TO_BINARY('0xFFFF'), HEX_TO_BINARY('0xF00F'), HEX_TO_BINARY('0xF00F'), HEX_TO_BINARY('0xF00F'))"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    df.cache()
    df.show()
    println("schema =" + df.schema)
    val c = df.count()
    println("count = " + c)

    assert(c == n)

    stmt.execute("drop table " + tableName1)
  }

  it should "load data from Vertica for [all Boolean data types] of Vertica" in {
    val tableName1 = "t1"

    val n = 40
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a boolean, b boolean)")
    val insert = "insert into "+ tableName1 + " values('t',0)"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    df.cache()
    df.show()
    println("schema =" + df.schema)
    val c = df.count()
    println("count = " + c)

    assert(c == n)

    stmt.execute("drop table " + tableName1)
  }

  it should "load data from Vertica for [all Character data types] of Vertica" in {
    val tableName1 = "t1"

    val n = 40
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a CHARACTER , b CHARACTER(10), c VARCHAR (20), d  CHARACTER VARYING(30) )")
    val insert = "insert into "+ tableName1 + " values('a', 'efghi', 'jklm', 'nopqrst')"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    df.cache()
    df.show()
    println("schema =" + df.schema)
    val c = df.count()
    println("count = " + c)

    assert(c == n)

    stmt.execute("drop table " + tableName1)
  }

  it should "load data from Vertica for [all Date/Time data types] of Vertica" in {
    val tableName1 = "t1"

    val n = 40
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a DATE , b TIME (10), c TIMETZ (20), d  TIMESTAMP, e TIMESTAMPTZ , f INTERVAL DAY TO SECOND, g  INTERVAL YEAR TO MONTH  )")
    val insert = "insert into "+ tableName1 + " values('1/8/1999', '2004-10-19 10:23:54', '23:59:59.999999-14', '2004-10-19 10:23:54', '2004-10-19 10:23:54+02', '1 day 6 hours', '1 year 6 months')"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    df.cache()
    df.show()
    println("schema =" + df.schema)
    val c = df.count()
    println("count = " + c)

    assert(c == n)

    stmt.execute("drop table " + tableName1)
  }

  it should "load data from Vertica for [all Long data types] of Vertica" in {
    val tableName1 = "t1"

    val n = 40
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a LONG VARBINARY(100) , b LONG VARCHAR  (120)  )")
    val insert = "insert into "+ tableName1 + " values('abcde', 'fghijk')"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    df.cache()
    df.show()
    println("schema =" + df.schema)
    val c = df.count()
    println("count = " + c)

    assert(c == n)

    stmt.execute("drop table " + tableName1)
  }

  it should "load data from Vertica for [Int data types] of Vertica" in {
    val tableName1 = "t1"

    val n = 10
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a INTEGER  , b SMALLINT , c BIGINT, d INT8     )")
    val insert = "insert into "+ tableName1 + " values(1,2,3,4)"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    df.cache()
    df.show()
    println("schema =" + df.schema)
    val c = df.count()
    println("count = " + c)

    assert(c == n)

    stmt.execute("drop table " + tableName1)
  }

  it should "load data from Vertica for [Double data types] of Vertica" in {
    val tableName1 = "t1"

    val n = 10
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a DOUBLE PRECISION, b FLOAT, c FLOAT(20), d FLOAT8, e REAL    )")
    val insert = "insert into "+ tableName1 + " values(1.1, 2.2, 3.3, 4.4, 5.5)"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    df.cache()
    df.show()
    println("schema =" + df.schema)
    val c = df.count()
    println("count = " + c)

    assert(c == n)

    stmt.execute("drop table " + tableName1)
  }

  it should "load data from Vertica for [Numeric data types] of Vertica" in {
    val tableName1 = "t1"

    val n = 10
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a NUMERIC(5,2), b DECIMAL, c NUMBER, d MONEY(6,3)     )")
    val insert = "insert into "+ tableName1 + " values(1.1, 2.2, 3.3, 4.4)"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    df.cache()
    df.show()
    println("schema =" + df.schema)
    val c = df.count()
    println("count = " + c)

    assert(c == n)

    stmt.execute("drop table " + tableName1)
  }

  it should "load data from Vertica with a DATE-type pushdown filter" in {
    val tableName1 = "dftest1"
    val stmt = conn.createStatement()
    val n = 3

    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a DATE, b float)")

    var insert = "insert into "+ tableName1 + " values('1977-02-01', 2.2)"
    TestUtils.populateTableBySQL(stmt, insert, n)
    insert = "insert into "+ tableName1 + " values('2077-02-01', 2.2)"
    TestUtils.populateTableBySQL(stmt, insert, n + 1)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    val r = df.filter("a < cast('2001-01-01' as DATE)").count
    val r2 = df.filter("a > cast('2001-01-01' as DATE)").count
    assert(r == n)
    assert(r2 == (n + 1))

    stmt.execute("drop table " + tableName1)
  }

  it should "load data from Vertica with a String-type pushdown filter" in {
    val tableName1 = "dftest1"
    val stmt = conn.createStatement()
    val n = 3

    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a varchar(10), b float)")

    var insert = "insert into "+ tableName1 + " values('abc', 2.2)"
    TestUtils.populateTableBySQL(stmt, insert, n)
    insert = "insert into "+ tableName1 + " values('cde', 2.2)"
    TestUtils.populateTableBySQL(stmt, insert, n + 1)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    val r = df.filter("a = 'abc'").count
    val r2 = df.filter("a = 'cde'").count
    assert(r == n)
    assert(r2 == (n + 1))

    stmt.execute("drop table " + tableName1)
  }

  it should "load data from Vertica with a TIMESTAMP-type pushdown filter" in {
    val tableName1 = "dftest1"
    val stmt = conn.createStatement()
    val n = 3

    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a TIMESTAMP, b float)")

    var insert = "insert into "+ tableName1 + " values(TIMESTAMP '2010-03-25 12:47:32.62', 2.2)"
    TestUtils.populateTableBySQL(stmt, insert, n)
    insert = "insert into "+ tableName1 + " values(TIMESTAMP '2010-03-25 12:55:49.123456', 2.2)"
    TestUtils.populateTableBySQL(stmt, insert, n + 1)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    val dr = df.filter("a = cast('2010-03-25 12:55:49.123456' AS TIMESTAMP)")
    val r = dr.count
    assert(r == n + 1)

    dr.show
    stmt.execute("drop table " + tableName1)
  }

  it should "fetch the correct results when startsWith and endsWith functions are used" in {
    val tableName1 = "dftest1"

    val n = 10
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a varchar, b integer)")
    var insert = "insert into "+ tableName1 + " values('christmas', 5)"
    TestUtils.populateTableBySQL(stmt, insert, n/2)

    insert = "insert into "+ tableName1 + " values('hannukah', 10)"
    TestUtils.populateTableBySQL(stmt, insert, n/2)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    val r = df.filter(df("a").startsWith("chr")).count

    assert(r == n/2)

    val s = df.filter(df("a").endsWith("kah")).count
    assert(s == n/2)

    stmt.execute("drop table " + tableName1)
  }


  it should "fetch the correct results when custom, non-integer segmentation is used" in {
    val tableName1 = "custom_segexpr_table"

    val n = 10
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a varchar, b integer) segmented by mod(b, 3) all nodes")
    var insert = "insert into "+ tableName1 + " values('christmas', NULL)"
    TestUtils.populateTableBySQL(stmt, insert, 1)

    insert = "insert into "+ tableName1 + " values('hannukah', 10)"
    TestUtils.populateTableBySQL(stmt, insert, n-1)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    val r = df.cache.count

    assert(r == n)

    val s = df.filter("b is NULL").count

    assert(s == 1)

    stmt.execute("drop table " + tableName1)
  }

  it should "work when using isin or in" in {
    val tableName1 = "test_in_clause"

    val (i,j,n) = (2,3,5)
    val stmt = conn.createStatement
    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a varchar, b integer)")
    var insert = "insert into "+ tableName1 + " values('christmas', 5)"
    TestUtils.populateTableBySQL(stmt, insert, i)

    insert = "insert into "+ tableName1 + " values('hannukah', 10)"
    TestUtils.populateTableBySQL(stmt, insert, j)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    val r = df.filter("a in ('christmas','hannukah','foo')").count
    val u = df.filter(df.col("a").isin("christmas","hannukah","foo")).count

    assert(r == n)
    assert(u == n)

    val s = df.filter("b in (3,4,5)").count
    val t = df.filter("b in (2,6,10)").count

    assert(s == i)
    assert(t == j)

    stmt.execute("drop table " + tableName1)
  }

  it should "be able to handle interval types" in {
    val tableName1 = "dftest"
    val stmt = conn.createStatement()
    val n = 1

    TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (f INTERVAL DAY TO SECOND, g INTERVAL YEAR TO MONTH)")

    var insert = "insert into "+ tableName1 + " values('1 day 6 hours', '1 year 6 months')"
    TestUtils.populateTableBySQL(stmt, insert, n)

    val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

    assert(df.cache.count == n)
    stmt.execute("drop table " + tableName1)
  }



}
