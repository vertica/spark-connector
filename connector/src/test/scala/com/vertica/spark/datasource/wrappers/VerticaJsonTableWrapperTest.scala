package com.vertica.spark.datasource.wrappers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.v2.json.JsonTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class VerticaJsonTableWrapperTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  private val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test")
    .getOrCreate()

  class MockJsonTable(_name: String,
                      sparkSession: SparkSession,
                      options: CaseInsensitiveStringMap,
                      paths: Seq[String],
                      userSpecifiedSchema: Option[StructType],
                      fallbackFileFormat: Class[_ <: FileFormat])
    extends JsonTable(_name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat) {

    override lazy val schema: StructType = StructType(Seq())
  }

  private val mockTable = new MockJsonTable("MockJsonTable", spark, CaseInsensitiveStringMap.empty(), List(), Some(StructType(Seq())), classOf[JsonFileFormat])

  behavior of "VerticaJsonTableTest"

  it should "return JsonTable capabilities" in {
    assert(new VerticaJsonTableWrapper(mockTable).capabilities() == mockTable.capabilities())
  }

  it should "build VerticaScanWrapperBuilder" in {
    assert(new VerticaJsonTableWrapper(mockTable).newScanBuilder(CaseInsensitiveStringMap.empty()).isInstanceOf[VerticaScanWrapperBuilder])
  }

  it should "return JsonTable name" in {
    assert(new VerticaJsonTableWrapper(mockTable).name() == "Vertica" + mockTable.name)
  }

  it should "return JsonTable schema" in {
    // Comparing references
    assert(new VerticaJsonTableWrapper(mockTable).schema() == mockTable.schema)
  }

  override protected def afterAll(): Unit = spark.close()
}
