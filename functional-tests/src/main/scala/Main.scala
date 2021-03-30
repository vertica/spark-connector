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

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import com.vertica.spark.config.{DistributedFilesystemReadConfig, FileStoreConfig, JDBCConfig, TableName, VerticaMetadata}
import com.vertica.spark.functests.{CleanupUtilTests, EndToEndTests, HDFSTests, JDBCTests}
import ch.qos.logback.classic.Level

object Main extends App {
  val conf: Config = ConfigFactory.load()

  val jdbcConfig = JDBCConfig(host = conf.getString("functional-tests.host"),
                              port = conf.getInt("functional-tests.port"),
                              db = conf.getString("functional-tests.db"),
                              username = conf.getString("functional-tests.user"),
                              password = conf.getString("functional-tests.password"),
                              logLevel= if(conf.getBoolean("functional-tests.log")) Level.DEBUG else Level.OFF)

  new JDBCTests(jdbcConfig).execute()

  /*
  val filename = conf.getString("functional-tests.filepath")
  val dirTestFilename = conf.getString("functional-tests.dirpath")
  new HDFSTests(
    FileStoreConfig(
      filename,
      logLevel = if(conf.getBoolean("functional-tests.log")) Level.ERROR else Level.OFF
    ),
    FileStoreConfig(dirTestFilename,
      logLevel = if(conf.getBoolean("functional-tests.log")) Level.ERROR else Level.OFF
    ),
    jdbcConfig
  ).execute()

  new CleanupUtilTests(
    FileStoreConfig(filename,
      logLevel = if(conf.getBoolean("functional-tests.log")) Level.ERROR else Level.OFF
    )
  ).execute()

  val readOpts = Map(
    "host" -> conf.getString("functional-tests.host"),
    "user" -> conf.getString("functional-tests.user"),
    "db" -> conf.getString("functional-tests.db"),
    "staging_fs_url" -> conf.getString("functional-tests.filepath"),
    "password" -> conf.getString("functional-tests.password"),
    "logging_level" -> {if(conf.getBoolean("functional-tests.log")) "DEBUG" else "OFF"}
  )
  val writeOpts = readOpts
  new EndToEndTests(readOpts, writeOpts).execute()

   */
}
