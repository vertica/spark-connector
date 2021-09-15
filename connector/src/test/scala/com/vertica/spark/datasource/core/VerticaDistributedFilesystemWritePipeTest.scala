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

package com.vertica.spark.datasource.core

import java.sql.ResultSet

import com.vertica.spark.common.TestObjects
import com.vertica.spark.config.{BasicJdbcAuth, DistributedFilesystemWriteConfig, FileStoreConfig, JDBCConfig, JDBCTLSConfig, TableName, ValidColumnList, ValidFilePermissions}
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.{CommitError, ConnectionDownError, ConnectorError, FaultToleranceTestFail, JdbcSchemaError, OpenWriteError, SchemaConversionError, SyntaxError, ViewExistsError}
import com.vertica.spark.util.schema.SchemaToolsInterface
import com.vertica.spark.util.table.TableUtilsInterface
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class VerticaDistributedFilesystemWritePipeTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {
  private val tablename = TableName("dummy", None)
  private val tempTableName = TableName("dummy_id", None)
  private val jdbcConfig = JDBCConfig(
    "1.1.1.1", 1234, "test", BasicJdbcAuth("test", "test"), JDBCTLSConfig(tlsMode = Require, None, None, None, None))
  private val fileStoreConfig = TestObjects.fileStoreConfig
  private val strlen = 1024

  val schema = new StructType(Array(StructField("col1", IntegerType)))

  private def createWriteConfig() = {
    DistributedFilesystemWriteConfig(
      jdbcConfig = jdbcConfig,
      fileStoreConfig = fileStoreConfig,
      tablename = tablename,
      schema = schema,
      strlen = strlen,
      targetTableSql = None,
      copyColumnList = None,
      sessionId = "id",
      0.0f,
      ValidFilePermissions("777").getOrElse(throw new Exception("File perm error")),
      false
    )
  }

  private def checkResult(result: ConnectorResult[Unit]): Unit = {
    result match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
  }

  private def getCountTableResultSet(count: Int = 0): ResultSet = {
    val resultSet = mock[ResultSet]
    (resultSet.next _).expects().returning(true)
    (resultSet.getInt(_: String)).expects("count").returning(count)
    (resultSet.close _).expects().returning(())

    resultSet
  }

  private def getEmptyResultSet: ResultSet = {
    val resultSet = mock[ResultSet]
    (resultSet.close _).expects().returning()

    resultSet
  }

  private def tryDoPreWriteSteps(pipe: VerticaDistributedFilesystemWritePipe) {
    pipe.doPreWriteSteps() match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
  }

}