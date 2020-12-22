package com.vertica.spark.functests

object Main extends App {
  (new JDBCTests).execute()
}
