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

package com.vertica.spark.util.version

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.{ComplexTypeColumnsNotSupported, ErrorList}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, Metadata, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.ResultSet

object VerticaVersionUtilsTest extends VerticaVersionUtilsTest {
  def mockGetVersion(jdbcLayer: JdbcLayerInterface): Unit = {
    val mockRs = mock[ResultSet]
    (jdbcLayer.query _).expects("SELECT version();", *).returns(Right(mockRs))
    (mockRs.next _).expects().returning(true)
    (mockRs.getString: Int => String).expects(1).returns(" Vertica Analytic Database v11.1.2-3")
    (mockRs.close _).expects()
  }

  def mockFailedGetVersion(jdbcLayer: JdbcLayerInterface): Unit = {
    val mockRs = mock[ResultSet]
    (jdbcLayer.query _).expects("SELECT version();", *).returns(Right(mockRs))
    (mockRs.next _).expects().returning(false)
    (mockRs.close _).expects()
    (mockRs.close _).expects()
  }
}

class VerticaVersionUtilsTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {
  private val complexTypesSchema: StructType = StructType(Array(
    StructField("ct1", ArrayType(IntegerType, false), false, Metadata.empty),
    StructField("ct2", StructType(Array(StructField("element",IntegerType, false, Metadata.empty))), false, Metadata.empty),
    StructField("ct3", MapType(IntegerType, IntegerType, false), false, Metadata.empty),
    StructField("col2", IntegerType, false, Metadata.empty)))

  private val nativeArraySchema: StructType = StructType(Array(
    StructField("ct1", ArrayType(IntegerType, false), false, Metadata.empty),
    StructField("col2", IntegerType, false, Metadata.empty)))

  private val complexArraySchema: StructType = StructType(Array(
    StructField("ct1", ArrayType(ArrayType(IntegerType, false), false), false, Metadata.empty),
    StructField("col2", IntegerType, false, Metadata.empty)))

  it should "Obtain a Vertica version number" in {
    val jdbcLayer = mock[JdbcLayerInterface]
    VerticaVersionUtilsTest.mockGetVersion(jdbcLayer)

    val version = VerticaVersionUtils.getVersion(jdbcLayer)
    assert(version.major == 11)
    assert(version.minor == 1)
    assert(version.servicePack == 2)
    assert(version.hotfix == 3)
  }

  it should "Obtain the default Vertica version number on failure" in {
    val jdbcLayer = mock[JdbcLayerInterface]
    val mockRs = mock[ResultSet]
    (jdbcLayer.query _).expects("SELECT version();", *).returns(Right(mockRs))
    (mockRs.next _).expects().returning(false)
    (mockRs.close _).expects()
    (mockRs.close _).expects()

    val version = VerticaVersionUtils.getVersion(jdbcLayer)
    assert(version.compare(VerticaVersionUtils.LATEST) == 0)
  }

  it should "Deny writing complex types to Vertica version =< 9" in {
    VerticaVersionUtils.checkSchemaTypesWriteSupport(complexTypesSchema, writingToExternal = false,
      VerticaVersion(9)) match {
      case Right(_) => fail
      case Left(error) => assert(error.isInstanceOf[ComplexTypeColumnsNotSupported])
    }
  }

  it should "Deny writing complex arrays to internal tables in Vertica 10" in {
    VerticaVersionUtils.checkSchemaTypesWriteSupport(complexArraySchema, writingToExternal = false,
      VerticaVersion(10)) match {
      case Right(_) => fail
      case Left(error) => assert(error.isInstanceOf[ErrorList])
    }
  }

  it should "Allow writing complex arrays to external tables in Vertica 10" in {
    VerticaVersionUtils.checkSchemaTypesWriteSupport(complexArraySchema, writingToExternal = true,
      VerticaVersion(10)) match {
      case Right(_) => succeed
      case Left(error) => fail
    }
  }

  it should "Allow writing native arrays to tables in Vertica 10" in {
    VerticaVersionUtils.checkSchemaTypesWriteSupport(nativeArraySchema, writingToExternal = false,
      VerticaVersion(10)) match {
      case Right(_) => succeed
      case Left(error) => fail
    }
  }

  it should "Deny reading complex types to Vertica version =< 9" in {
    VerticaVersionUtils.checkSchemaTypesReadSupport(complexTypesSchema,
      VerticaVersion(9)) match {
      case Right(_) => fail
      case Left(error) => assert(error.isInstanceOf[ComplexTypeColumnsNotSupported])
    }
  }

  it should "Allow reading native arrays from Vertica >= 11" in {
    VerticaVersionUtils.checkSchemaTypesReadSupport(nativeArraySchema,
      VerticaVersion(11)) match {
      case Right(_) => succeed
      case Left(error) => fail
    }
  }

  it should "Deny reading complex types from Vertica >= 11" in {
    VerticaVersionUtils.checkSchemaTypesReadSupport(complexTypesSchema,
      VerticaVersion(11)) match {
      case Right(_) => fail
      case Left(error) => assert(error.isInstanceOf[ErrorList])
    }
  }

  it should "Deny reading complex arrays from Vertica 10" in {
    VerticaVersionUtils.checkSchemaTypesReadSupport(complexArraySchema,
      VerticaVersion(10)) match {
      case Right(_) => fail
      case Left(error) => assert(error.isInstanceOf[ErrorList])
    }
  }

  it should "Allow reading native arrays from Vertica 10" in {
    VerticaVersionUtils.checkSchemaTypesReadSupport(nativeArraySchema,
      VerticaVersion(10)) match {
      case Right(_) => succeed
      case Left(error) => fail
    }
  }
}
