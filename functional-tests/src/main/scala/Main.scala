import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import com.vertica.spark.config.{DistributedFilesystemReadConfig, FileStoreConfig, JDBCConfig, TableName, VerticaMetadata}
import com.vertica.spark.functests.{EndToEndTests, HDFSTests, JDBCTests}
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

  val filename = conf.getString("functional-tests.filepath")
  val dirTestFilename = conf.getString("functional-tests.dirpath")
  new HDFSTests(
    DistributedFilesystemReadConfig(
      logLevel = if(conf.getBoolean("functional-tests.log")) Level.ERROR else Level.OFF,
      jdbcConfig,
      FileStoreConfig(filename, Level.ERROR),
      TableName("", None),
      None,
      None
    ),
    DistributedFilesystemReadConfig(
      if(conf.getBoolean("functional-tests.log")) Level.ERROR else Level.OFF,
      jdbcConfig,
      FileStoreConfig(dirTestFilename, Level.ERROR),
      TableName("", None),
      None,
      None
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
  new EndToEndTests(readOpts).execute()
}
