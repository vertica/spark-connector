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
import com.vertica.spark.util.error.{ComplexTypeReadNotSupported, ComplexTypeWriteNotSupported, ExportToJsonNotSupported, InternalMapNotSupported, NativeArrayWriteNotSupported}
import org.apache.spark.sql.types._
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
  private val rowsAndArraysSchema: StructType = StructType(Array(
    StructField("ct1", ArrayType(ArrayType(IntegerType)), false, Metadata.empty),
    StructField("ct2", StructType(Array(StructField("element",IntegerType, false, Metadata.empty))), false, Metadata.empty),
    StructField("col1", IntegerType, false, Metadata.empty)))

  private val primitiveTypeSchema: StructType = StructType(Array(
    StructField("col1", IntegerType, false, Metadata.empty)))

  private val nativeArraySchema: StructType = StructType(Array(
    StructField("ct1", ArrayType(IntegerType, false), false, Metadata.empty)))

  private val mapSchema: StructType = StructType(Array(
    StructField("ct1", MapType(IntegerType, IntegerType))
  ))

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
    assert(version.compare(VerticaVersionUtils.VERRTICA_LATEST) == 0)
  }

  it should "Allow writing primitive" in {
    (1 to VerticaVersionUtils.VERRTICA_LATEST.major).foreach(i => {
      VerticaVersionUtils.checkSchemaTypesWriteSupport(primitiveTypeSchema, Version(i), toInternalTable = true) match {
        case Right(_) =>
        case Left(err) => fail(err.toString)
      }
    })
  }

  it should "Error on writing arrays and rows types to Vertica version <= 9" in {
    (1 to 9).foreach(i => {
      VerticaVersionUtils.checkSchemaTypesWriteSupport(rowsAndArraysSchema, Version(i), toInternalTable = true) match {
        case Right(_) => fail
        case Left(err) =>
          assert(err.isInstanceOf[ComplexTypeWriteNotSupported])
          assert(err.asInstanceOf[ComplexTypeWriteNotSupported].colList.length == 2)
      }
    })
  }

  it should "Error on writing native arrays to Vertica version <= 9" in {
    (1 to 9).foreach(i => {
      VerticaVersionUtils.checkSchemaTypesWriteSupport(nativeArraySchema, Version(i), toInternalTable = true) match {
        case Right(_) => fail
        case Left(err) =>
          assert(err.isInstanceOf[NativeArrayWriteNotSupported])
          assert(err.asInstanceOf[NativeArrayWriteNotSupported].colList.length == 1)
      }
    })
  }

  it should "Allow writing native array to Vertica 10" in {
    // scalastyle:off
    VerticaVersionUtils.checkSchemaTypesWriteSupport(nativeArraySchema, Version(10), toInternalTable = true) match {
      case Right(_) => succeed
      case Left(_) => fail
    }
  }

  it should "Error on writing arrays and rows to Vertica 10" in {
    VerticaVersionUtils.checkSchemaTypesWriteSupport(rowsAndArraysSchema, Version(10), toInternalTable = true) match {
      case Right(_) => fail
      case Left(err) =>
        assert(err.isInstanceOf[ComplexTypeWriteNotSupported])
        assert(err.asInstanceOf[ComplexTypeWriteNotSupported].colList.length == 2)
    }
  }

  it should "Allow writing arrays and rows to Vertica 11" in {
    VerticaVersionUtils.checkSchemaTypesWriteSupport(rowsAndArraysSchema, Version(11), toInternalTable = true) match {
      case Right(_) => succeed
      case Left(err) => fail(err.toString)
    }
  }

  it should "Allow writing native array to Vertica 11" in {
    VerticaVersionUtils.checkSchemaTypesWriteSupport(nativeArraySchema, Version(11), toInternalTable = true) match {
      case Right(_) => succeed
      case Left(err) => fail(err.toString)
    }
  }

  it should "Error on writing Map to internal tables in Vertica 11" in {
    VerticaVersionUtils.checkSchemaTypesWriteSupport(mapSchema, Version(11), toInternalTable = true) match {
      case Right(_) => fail()
      case Left(err) =>
        assert(err.isInstanceOf[InternalMapNotSupported])
    }
  }

  it should "Allow writing Map to external tables in Vertica 11" in {
    VerticaVersionUtils.checkSchemaTypesWriteSupport(rowsAndArraysSchema, Version(11), toInternalTable = false) match {
      case Right(_) => succeed
      case Left(err) => fail(err.getFullContext)
    }
  }

  it should "Allow reading primitive types" in {
    (1 to VerticaVersionUtils.VERRTICA_LATEST.major).foreach(i => {
      VerticaVersionUtils.checkSchemaTypesReadSupport(primitiveTypeSchema, Version(i)) match {
        case Right(_) =>
        case Left(err) => fail(err.toString)
      }
    })
  }

  it should "Error on reading arrays and rows" in {
    (1 to VerticaVersionUtils.VERRTICA_LATEST.major).foreach(i => {
      VerticaVersionUtils.checkSchemaTypesReadSupport(rowsAndArraysSchema, Version(i)) match {
        case Right(_) => fail
        case Left(err) =>
          assert(err.isInstanceOf[ComplexTypeReadNotSupported])
          assert(err.asInstanceOf[ComplexTypeReadNotSupported].colList.length == 2)
      }
    })
  }

  it should "Allow reading native array on Vertica 11" in {
    VerticaVersionUtils.checkSchemaTypesReadSupport(nativeArraySchema, Version(11)) match {
      case Right(_) => succeed
      case Left(err) => fail
    }
  }

  it should "block version < 11.1.1 when checking for Json export support" in {
    assert(VerticaVersionUtils.checkJsonSupport(Version(11,1,1)) == Right())
    assert(VerticaVersionUtils.checkJsonSupport(Version(12)) == Right())
    assert(VerticaVersionUtils.checkJsonSupport(Version(11,1)) == Left(ExportToJsonNotSupported(Version(11,1).toString)))
    assert(VerticaVersionUtils.checkJsonSupport(Version(11)) == Left(ExportToJsonNotSupported(Version(11).toString)))
  }

}

class VersionTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {

  it should "compare to bigger version" in {
    //scalastyle:off
    assert(Version(11,1,5,3).largerThan(Version(10,4,7,5)))
  }

  it should "compare to smaller version" in {
    //scalastyle:off
    assert(Version(11,1,5,3).lessThan(Version(12,0,2,1)))
  }

  it should "compare to smaller or equal versions" in {
    assert(Version(11,1,5,3).lesserOrEqual(Version(11,1,5,3)))
    assert(Version(11,1,5,3).lesserOrEqual(Version(11,2,5,3)))
  }

  it should "compare to bigger or equal versions" in {
    assert(Version(11,1,5,3).largerOrEqual(Version(11,1,5,3)))
    assert(Version(11,1,5,3).largerOrEqual(Version(11,1,5,2)))
  }
}


