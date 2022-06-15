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

import com.vertica.spark.datasource.wrappers.VerticaScanWrapperBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.v2.json.JsonTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util


/**
 * Wraps a [[JsonTable]] so that that it will create a [[VerticaScanWrapperBuilder]].
 * */
class VerticaJsonTableWrapper(val jsonTable: JsonTable) extends Table with SupportsRead {
  override def name(): String = "Vertica" + jsonTable.name

  override def schema(): StructType = jsonTable.schema

  override def capabilities(): util.Set[TableCapability] = jsonTable.capabilities()

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder =
    new VerticaScanWrapperBuilder(jsonTable.newScanBuilder(caseInsensitiveStringMap))
}
