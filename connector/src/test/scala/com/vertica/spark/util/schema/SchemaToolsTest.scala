import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import com.vertica.spark.util.schema._
import com.vertica.spark.jdbc._

import org.scalamock.scalatest.MockFactory
import java.sql.ResultSet
import java.sql.ResultSetMetaData

class SchemaToolsTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory{

  it should "parse a basic 1-column schema" in {
    val tablename = "testtable"

    val jdbcLayer = mock[JdbcLayerInterface]
    val resultSet = mock[ResultSet]
    val rsmd = mock[ResultSetMetaData]

    (jdbcLayer.query _).expects("SELECT * FROM testtable WHERE 1=0").returning(Right(resultSet))
    (resultSet.getMetaData _).expects().returning(rsmd)
    (resultSet.close _).expects()

    // Schema
    (rsmd.getColumnCount _).expects().returning(1)
    (rsmd.getColumnLabel _).expects(1).returning("col1")
    //(rsmd.getColumnType  _).expects().returning("col1")

    SchemaTools.readSchema(jdbcLayer, tablename)
    assert(true)
  }
}
