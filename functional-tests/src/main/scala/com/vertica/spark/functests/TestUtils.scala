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

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import com.vertica.spark.config.{BasicJdbcAuth, JDBCConfig, KerberosAuth}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object TestUtils {
  def getJDBCConnection(config: JDBCConfig): Connection = {
    Class.forName("com.vertica.jdbc.Driver").newInstance()

    config.auth match {
      case BasicJdbcAuth(username, password) =>
        val prop = new Properties()
        prop.put("user", username)
        prop.put("password", password)

        getConnectionByProp(config.host, config.port, config.db, prop)(None)
      case KerberosAuth(username, kerberosServiceName, kerberosHostname, jaasConfigName) =>
        val prop = new Properties()
        prop.put("user", username)
        prop.put("KerberosServiceName", kerberosServiceName)
        prop.put("KerberosHostname", kerberosHostname)
        prop.put("JAASConfigName", jaasConfigName)

        getConnectionByProp(config.host, config.port, config.db, prop)(None)
    }
  }

  def getJDBCUrl(host: String, port: Int = 5433, db: String): String = {
    "jdbc:vertica://" + host + ":" + port + "/" + db
  }

  def getConnectionByProp(host: String, port: Int = 5433, db: String, prop: Properties)(overrideHost: Option[String]): Connection = {
    Class.forName("com.vertica.jdbc.Driver").newInstance()

    val h = overrideHost match {
      case Some(m) => m
      case None    => host
    }
    prop.setProperty("host", h)
    val jdbcURI = getJDBCUrl(h, port, db)
    DriverManager.getConnection(jdbcURI, prop)

  }

  def createTableBySQL(conn: Connection, tableName: String, createTableStr: String): Boolean = {
    val stmt = conn.createStatement()

    stmt.execute("drop table if exists " + tableName)
    println(createTableStr)
    stmt.execute(createTableStr)
  }

  def dropTable(conn: Connection, tableName: String, schema: Option[String] = None): Unit = {
    val stmt = conn.createStatement()
    schema match {
      case Some(dbschema) => stmt.execute("drop table if exists \"" + dbschema + "\".\"" + tableName + "\" cascade")
      case None => stmt.execute("drop table if exists \"" + tableName + "\" cascade")
    }
  }

  def createTable(conn: Connection, tableName: String, isSegmented: Boolean = true, numOfRows: Int = 10): Unit = {
    val stmt = conn.createStatement()
    stmt.execute("drop table if exists " + tableName)
    val createStr = "create table " + tableName + "(a int, b int) " +
      (if (isSegmented) "segmented by hash(a)" else "unsegmented") + " all nodes;"
    println(createStr)
    stmt.execute(createStr)
    populateTable(stmt, tableName, numOfRows)
  }

  private def populateTable(stmt: Statement, tableName: String, numOfRows: Int) {
    for (i <- 0 until numOfRows) {
      val insertStr = "insert into " + tableName + " values (" + i + " ," + i + ")"
      println(insertStr)
      stmt.execute(insertStr)
    }
  }

  def populateTableBySQL(stmt: Statement, insertStr: String, numOfRows: Int) {
    for (_ <- 0 until numOfRows) {
      stmt.execute(insertStr)
    }
  }

  def doCount(spark: SparkSession, opt:Map[String, String]):Long = {
    val df = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(opt).load()

    df.cache()
    df.show()
    println("schema =" + df.schema)
    val c = df.count()
    println("count = " + c)
    c
  }

  def createTablePartialNodes(conn: Connection, tableName: String, isSegmented: Boolean = true, numOfRows: Int = 10, nodes: List[String]): Unit = {
    val stmt = conn.createStatement()
    stmt.execute("drop table if exists " + tableName)
    val createStr = "create table if not exists " + tableName + "(a int, b int) " +
      (if (isSegmented) "segmented by hash(a)" + " nodes " + nodes.mkString(",")
      else " UNSEGMENTED node " + nodes.head + " KSAFE 0") + ";"
    println(createStr)
    stmt.execute(createStr)
    populateTable(stmt, tableName, numOfRows)
  }

  def getNodeNames(conn: Connection): List[String] = {
    val nodeQry = "select node_name from nodes;"
    val rs = conn.createStatement().executeQuery(nodeQry)
    new Iterator[String] {
      def hasNext: Boolean = rs.next()
      def next(): String = rs.getString(1)
    }.toList
  }

  def getKmeans100colFloatSchema: org.apache.spark.sql.types.StructType = {
    val colNames = (for(i<-Range(1,101)) yield "feature"+i).toArray
    val nullable = false
    val cols = for (i <-colNames.indices) yield StructField(colNames(i), org.apache.spark.sql.types.FloatType, nullable)
    val schema = StructType(cols.toArray)
    schema
  }

  def getKmeans100colFloatRowRDD(kmmdata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[org.apache.spark.sql.Row]  = {
    val rowRDD = kmmdata.map(_.split(" ").map(col=>col.trim.toFloat).toSeq).map(row=>Row.fromSeq(row))
    rowRDD
  }

}
