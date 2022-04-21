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

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.JdbcSchemaError
import com.vertica.spark.util.schema.SchemaToolsTests.{mockColumnCount, mockColumnMetadata, mockJdbcDeps, tablename}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.{ResultSet, ResultSetMetaData}

class SchemaToolsV10Test extends AnyFlatSpec with MockFactory with org.scalatest.OneInstancePerTest {
  it should "detects complex types in Vertica 10" in {
    val tableName = tablename.getFullTableName.replace("\"", "")
    val (jdbcLayer, _, rsmd) = mockJdbcDeps(tablename)
    val testColDef1 = TestColumnDef(1, "col1", java.sql.Types.LONGVARCHAR, "ARRAY", 0, signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef1)
    val testColDef2 = TestColumnDef(2, "col1", java.sql.Types.VARCHAR, "SET", 0, signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef2)
    val testColDef3 = TestColumnDef(3, "col2", java.sql.Types.LONGVARCHAR, "VERTICA_CT", 0, signed = false, nullable = true)
    mockColumnMetadata(rsmd, testColDef3)
    val colCount = 3
    mockColumnCount(rsmd, colCount)

    val verticaArrayType = 1506
    val mockRs2 = mock[ResultSet]
    val queryColType1 = s"SELECT data_type_id FROM columns WHERE table_name='$tableName' AND column_name='${testColDef1.name}'"
    (jdbcLayer.query _).expects(queryColType1, *).returning(Right(mockRs2))
    (jdbcLayer.close _).expects()
    (mockRs2.next _).expects().returning(true)
    (mockRs2.getLong: String => Long).expects("data_type_id").returning(verticaArrayType)
    (mockRs2.close _).expects()

    val verticaSetType = 2506
    val mockRs3 = mock[ResultSet]
    val queryColType2 = s"SELECT data_type_id FROM columns WHERE table_name='$tableName' AND column_name='${testColDef2.name}'"
    (jdbcLayer.query _).expects(queryColType2, *).returning(Right(mockRs3))
    (jdbcLayer.close _).expects()
    (mockRs3.next _).expects().returning(true)
    (mockRs3.getLong: String => Long).expects("data_type_id").returning(verticaSetType)
    (mockRs3.close _).expects()

    val verticaCT = 1234567890
    val queryColType3 = s"SELECT data_type_id FROM columns WHERE table_name='$tableName' AND column_name='${testColDef3.name}'"
    val mockRs4 = mock[ResultSet]
    (jdbcLayer.query _).expects(queryColType3, *).returning(Right(mockRs4))
    (jdbcLayer.close _).expects()
    (mockRs4.next _).expects().returning(true)
    (mockRs4.getLong: String => Long).expects("data_type_id").returning(verticaCT)
    (mockRs4.close _).expects()
    val queryComplexType = s"SELECT field_type_name FROM complex_types WHERE type_id='$verticaCT'"
    val mockRs5 = mock[ResultSet]
    (jdbcLayer.query _).expects(queryComplexType, *).returning(Right(mockRs5))
    (jdbcLayer.close _).expects()
    (mockRs5.next _).expects().returning(true)
    (mockRs5.close _).expects()

    new SchemaToolsV10().getColumnInfo(jdbcLayer, tablename) match {
      case Right(cols) =>
        assert(cols.length == colCount)
        assert(cols(0).label == testColDef1.name)
        assert(cols(0).colType == java.sql.Types.ARRAY)
        assert(cols(1).label == testColDef2.name)
        assert(cols(1).colType == java.sql.Types.ARRAY)
        assert(cols(2).label == testColDef3.name)
        assert(cols(2).colType == java.sql.Types.STRUCT)
      case Left(error) => fail(error.getFullContext)
    }
  }
}
