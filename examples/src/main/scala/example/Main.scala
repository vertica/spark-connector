import java.sql.Connection

object Main extends App {
  val conn: Connection = TestUtils.getJDBCConnection(readOpts("host"), db = readOpts("db"), user = readOpts("user"), password = readOpts("password"))

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  val tableName1 = "dftest"
  val stmt = conn.createStatement
  val n = 20
  TestUtils.createTableBySQL(conn, tableName1, "create table " + tableName1 + " (a int)")

  val insert = "insert into "+ tableName1 + " values(2)"
  TestUtils.populateTableBySQL(stmt, insert, n)

  val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("tablename" -> tableName1)).load()

  df.foreach(println)
}
