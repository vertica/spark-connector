import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import com.vertica.spark.datasource._

import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.expressions.Transform

import java.util

class VerticaV2SourceTests extends AnyFlatSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
  }

  it should "Return a Vertica Table" in {
    val source = new VerticaSource()
    val table = source.getTable(new StructType(), Array[Transform](), new util.HashMap[String, String]())

    assert(table.name == "VerticaTable")
  }

}
