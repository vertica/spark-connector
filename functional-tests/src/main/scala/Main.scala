package com.vertica.spark.functests

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import com.vertica.spark.config.JDBCConfig
import ch.qos.logback.classic.Level

object Main extends App {
  val conf: Config = ConfigFactory.load()

  val jdbcConfig = JDBCConfig(host = conf.getString("functional-tests.host"),
                              port = conf.getInt("functional-tests.port"),
                              db = conf.getString("functional-tests.db"),
                              username = conf.getString("functional-tests.user"),
                              password = conf.getString("functional-tests.password"),
                              logLevel= if(conf.getBoolean("functional-tests.log")) Level.DEBUG else Level.OFF)

  (new JDBCTests(jdbcConfig)).execute()
}
