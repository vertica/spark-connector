package com.vertica.spark.functests

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

object TestUtils {
  def getJDBCConnection(host: String, port: Int = 5433, db: String, user: String, password: String): Connection = {
    Class.forName("com.vertica.jdbc.Driver").newInstance()

    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", password)

    getConnectionByProp(host, port, db, prop)(None)
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


  def populateTableBySQL(stmt: Statement, insertStr: String, numOfRows: Int) {
    for (_ <- 0 until numOfRows) {
      stmt.execute(insertStr)
    }
  }
}
