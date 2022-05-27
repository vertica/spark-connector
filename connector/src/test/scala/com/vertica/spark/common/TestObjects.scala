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

package com.vertica.spark.common

import com.vertica.spark.config.{AWSOptions, BasicJdbcAuth, DistributedFilesystemWriteConfig, FileStoreConfig, GCSOptions, JDBCConfig, JDBCTLSConfig, TableName, ValidFilePermissions}
import com.vertica.spark.datasource.core.Require
import org.apache.spark.sql.types.StructType

object TestObjects {
  val fileStoreConfig: FileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/", "test", false, AWSOptions(None, None, None, None, None, None, None), GCSOptions(None, None, None))
  val tablename: TableName = TableName("testtable", None)
  val jdbcConfig: JDBCConfig = JDBCConfig(
    "1.1.1.1", 1234, "test", BasicJdbcAuth("test", "test"), JDBCTLSConfig(tlsMode = Require, None, None, None, None))
  val writeConfig: DistributedFilesystemWriteConfig = DistributedFilesystemWriteConfig(
    jdbcConfig = jdbcConfig,
    fileStoreConfig = fileStoreConfig,
    tablename = tablename,
    schema = new StructType(),
    targetTableSql = None,
    strlen = 1024,
    copyColumnList = None,
    sessionId = "id",
    failedRowPercentTolerance = 0.0f,
    filePermissions = ValidFilePermissions("777").getOrElse(throw new Exception("File perm error")),
    createExternalTable = None,
    saveJobStatusTable = false
  )
}
