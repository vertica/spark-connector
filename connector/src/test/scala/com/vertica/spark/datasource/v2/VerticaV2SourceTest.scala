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
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._

trait DummyReadPipe extends VerticaPipeInterface with VerticaPipeReadInterface

class VerticaV2SourceTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory{

  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
  }

  it should "return a Vertica Table" in {
    val source = new VerticaSource()
    val table = source.getTable(new StructType(), Array[Transform](), new util.HashMap[String, String]())

    assert(table.isInstanceOf[VerticaTable])
  }

  /* Temporarily disabled
  it should "return a Scan Builder" in {
    val opts = Map("logging_level" -> "ERROR",
                   "host" -> "1.1.1.1",
                   "port" -> "1234",
                   "db" -> "testdb",
                   "user" -> "user",
                   "password" -> "password",
                   "tablename" -> "tbl",
                   "staging_fs_url" -> "hdfs://test:8020/tmp/test"
                   )

    val source = new VerticaSource()
    val table = source.getTable(new StructType(), Array[Transform](), opts )
    val readTable = table.asInstanceOf[SupportsRead]
    val scanBuilder = readTable.newScanBuilder(new CaseInsensitiveStringMap(opts))

    assert(scanBuilder.isInstanceOf[VerticaScanBuilder])
  }
   */

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
