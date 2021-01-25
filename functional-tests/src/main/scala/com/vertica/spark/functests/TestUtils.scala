package com.vertica.spark.functests

import java.sql.Connection

object TestUtils {
  def createTableBySQL(conn: Connection, tableName: String, createTableStr: String): Boolean = {
    val stmt = conn.createStatement()

    stmt.execute("drop table if exists " + tableName)
    println(createTableStr)
    stmt.execute(createTableStr)
  }
}
