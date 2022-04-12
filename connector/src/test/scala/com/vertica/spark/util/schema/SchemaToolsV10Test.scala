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

package com.vertica.spark.util.schema

import com.vertica.spark.util.schema.SchemaToolsTests.{mockColumnCount, mockColumnMetadata, mockJdbcDeps, tablename}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class SchemaToolsV10Test extends AnyFlatSpec with MockFactory with org.scalatest.OneInstancePerTest {
  it should "detects complex types in Vertica 10" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)

    // Mocking SchemaTools.getColumnInfo
    val colName = "col1"
    mockColumnMetadata(rsmd, TestColumnDef(1, colName, java.sql.Types.VARCHAR, "VARCHAR", 0, signed = false, nullable = true))
    mockColumnCount(rsmd, 1)

    new SchemaToolsV10().getColumnInfo(jdbcLayer, tablename)
  }
}
