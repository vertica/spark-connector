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

import com.vertica.spark.util.query.VerticaTableTests
import com.vertica.spark.util.schema.SchemaToolsTests.{mockColumnCount, mockColumnMetadata, mockJdbcDeps, tablename}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.ResultSet

class SchemaToolsV10Test extends AnyFlatSpec with MockFactory with org.scalatest.OneInstancePerTest {

  it should "detects array type in Vertica 10 new" in {
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)
    val arrayTestColDef = TestColumnDef(1, "col1", java.sql.Types.LONGVARCHAR, "ARRAY", 0, signed = false, nullable = true)
    val setTestColDef = TestColumnDef(2, "col1", java.sql.Types.LONGVARCHAR, "ARRAY", 0, signed = false, nullable = true)
    val ctTestColDef = TestColumnDef(3, "col2", java.sql.Types.LONGVARCHAR, "VERTICA_CT", 0, signed = false, nullable = true)
    mockColumnMetadata(rsmd, arrayTestColDef)
    mockColumnMetadata(rsmd, setTestColDef)
    mockColumnMetadata(rsmd, ctTestColDef)
    mockColumnCount(rsmd, 3)

    val tbName = tablename.name.replaceAll("\"", "")
    val childTypeInfo = TestVerticaTypeDef(6, java.sql.Types.BIGINT, "childTypeName")
    val verticaArrayId = SchemaTools.VERTICA_NATIVE_ARRAY_BASE_ID + childTypeInfo.verticaTypeId
    val rootTypeDef = TestVerticaTypeDef(verticaArrayId, java.sql.Types.ARRAY, "rootTypeName", List(childTypeInfo))

    def mockArrayQuerying(): Unit = {
      // Query column type info
      VerticaTableTests.mockGetColumnInfo(arrayTestColDef.name, tbName, "", rootTypeDef.verticaTypeId, rootTypeDef.typeName, jdbcLayer)

      // Query element type info
      val (_, typesRs) = VerticaTableTests.mockGetTypeInfo(childTypeInfo.verticaTypeId, jdbcLayer)
      VerticaTableTests.mockTypeInfoResult(childTypeInfo.verticaTypeId, childTypeInfo.typeName, childTypeInfo.jdbcTypeId, typesRs)
      (typesRs.next _).expects().returning(false)
    }

    def mockSetQuerying(): Unit = {
      // Query column type info
      VerticaTableTests.mockGetColumnInfo(setTestColDef.name, tbName, "", rootTypeDef.verticaTypeId, rootTypeDef.typeName, jdbcLayer)

      // Query element type info
      val (_, setTypesRs) = VerticaTableTests.mockGetTypeInfo(childTypeInfo.verticaTypeId, jdbcLayer)
      VerticaTableTests.mockTypeInfoResult(childTypeInfo.verticaTypeId, childTypeInfo.typeName, childTypeInfo.jdbcTypeId, setTypesRs)
      (setTypesRs.next _).expects().returning(false)
    }

    def mockCtQuerying(): Unit = {
      val childTypeInfo = TestVerticaTypeDef(6, java.sql.Types.BIGINT, "childTypeName")
      val verticaCTId = 1234567890L
      val rootTypeDef = TestVerticaTypeDef(verticaCTId, java.sql.Types.STRUCT, "structTypeName", List(childTypeInfo))
      // Query column type info
      VerticaTableTests.mockGetColumnInfo(ctTestColDef.name, tbName, "", rootTypeDef.verticaTypeId, rootTypeDef.typeName, jdbcLayer)

      val (_, rs) = VerticaTableTests.mockGetComplexTypeInfo(rootTypeDef.verticaTypeId, jdbcLayer)
      VerticaTableTests.mockComplexTypeInfoResult(childTypeInfo.typeName, childTypeInfo.verticaTypeId, rootTypeDef.verticaTypeId, rs)
      (rs.next _).expects().returning(false)
    }

    mockArrayQuerying()
    mockSetQuerying()
    mockCtQuerying()

    new SchemaToolsV10().getColumnInfo(jdbcLayer, tablename) match {
      case Right(cols) =>
        assert(cols.length == 3)
        assert(cols.head.label == arrayTestColDef.name)
        assert(cols.head.colType == java.sql.Types.ARRAY)
        // Since v10 only need to return some complex type for error handling later on,
        // we do not care if it actually mark the array as a set or returns a correct
        // struct.
        assert(cols(1).label == setTestColDef.name)
        assert(cols(1).colType == java.sql.Types.ARRAY)
        assert(cols(2).label == ctTestColDef.name)
        assert(cols(2).colType == java.sql.Types.STRUCT)
      case Left(error) => fail(error.getFullContext)
    }
  }
}
