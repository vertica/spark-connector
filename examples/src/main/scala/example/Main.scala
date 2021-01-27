import java.sql.Connection
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import ch.qos.logback.classic.Level
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App {
  val conf: Config = ConfigFactory.load()

  val readOpts = Map(
    "host" -> conf.getString("functional-tests.host"),
    "user" -> conf.getString("functional-tests.user"),
    "db" -> conf.getString("functional-tests.db"),
    "staging_fs_url" -> conf.getString("functional-tests.filepath"),
    "password" -> conf.getString("functional-tests.password"),
    "logging_level" -> {if(conf.getBoolean("functional-tests.log")) "DEBUG" else "OFF"}
  )

  val conn: Connection = TestUtils.getJDBCConnection(readOpts("host"), db = readOpts("db"), user = readOpts("user"), password = readOpts("password"))

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  val tableName = "dftest"
  val stmt = conn.createStatement
  val n = 20
  TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int)")

  val insert = "insert into "+ tableName + " values(2)"
  TestUtils.populateTableBySQL(stmt, insert, n)

  val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()

  df.rdd.foreach(println)
}
