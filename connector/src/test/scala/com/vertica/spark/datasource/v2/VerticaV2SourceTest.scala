import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import com.vertica.spark.datasource._

import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.vertica.spark.datasource.v2._
import org.scalamock.scalatest.MockFactory

import java.util

import scala.collection.JavaConversions._

import com.vertica.spark.datasource.core._
import com.vertica.spark.config.VerticaMetadata

trait DummyReadPipe extends VerticaPipeInterface with VerticaPipeReadInterface

class VerticaV2SourceTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory{

  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
    VerticaPipeFactory.impl = new VerticaPipeFactoryDefaultImpl()
  }

  it should "return a Vertica Table" in {
    val source = new VerticaSource()
    val table = source.getTable(new StructType(), Array[Transform](), new util.HashMap[String, String]())

    assert(table.isInstanceOf[VerticaTable])
  }

  it should "return a Scan Builder" in {
    val opts = Map("logging_level" -> "ERROR",
                   "host" -> "1.1.1.1",
                   "port" -> "1234",
                   "db" -> "testdb",
                   "user" -> "user",
                   "password" -> "password",
                   "tablename" -> "tbl"
                   )
    // Set mock pipe
    val mockPipe = mock[DummyReadPipe]
    (mockPipe.getMetadata _).expects().returning(Right(VerticaMetadata(new StructType))).once()
    VerticaPipeFactory.impl = mock[VerticaPipeFactoryImpl]
    (VerticaPipeFactory.impl.getReadPipe _).expects(*).returning(mockPipe)

    val source = new VerticaSource()
    val table = source.getTable(new StructType(), Array[Transform](), opts )
    val readTable = table.asInstanceOf[SupportsRead]
    val scanBuilder = readTable.newScanBuilder(new CaseInsensitiveStringMap(opts))

    assert(scanBuilder.isInstanceOf[VerticaScanBuilder])
  }

  it should "return a Write Builder" in {
    val mockLogicalWriteInfo = mock[LogicalWriteInfo]

    val opts = Map("logging_level" -> "ERROR")
    val source = new VerticaSource()
    val table = source.getTable(new StructType(), Array[Transform](), opts )
    val writeTable = table.asInstanceOf[SupportsWrite]
    val writeBuilder = writeTable.newWriteBuilder(mockLogicalWriteInfo)


    assert(writeBuilder.isInstanceOf[VerticaWriteBuilder])
  }


}
