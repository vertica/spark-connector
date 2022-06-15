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
package com.vertica.spark.datasource.wrappers.json

import com.vertica.spark.common.TestObjects
import com.vertica.spark.datasource.wrappers.VerticaScanWrapperBuilder
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

  behavior of "VerticaJsonTableTest"

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
  private val readConfig = TestObjects.readConfig

  it should "return JsonTable capabilities" in {
    assert(new VerticaJsonTableWrapper(mockTable, readConfig).capabilities() == mockTable.capabilities())
  }

  it should "build VerticaScanWrapperBuilder" in {
    assert(new VerticaJsonTableWrapper(mockTable, readConfig).newScanBuilder(CaseInsensitiveStringMap.empty()).isInstanceOf[VerticaScanWrapperBuilder])
  }

  it should "return JsonTable name" in {
    assert(new VerticaJsonTableWrapper(mockTable, readConfig).name() == "Vertica" + mockTable.name)
  }

  it should "return JsonTable schema" in {
    // Comparing references
    assert(new VerticaJsonTableWrapper(mockTable, readConfig).schema() == mockTable.schema)
  }

  override protected def afterAll(): Unit = spark.close()
}
