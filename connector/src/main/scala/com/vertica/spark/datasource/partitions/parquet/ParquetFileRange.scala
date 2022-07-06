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

package com.vertica.spark.datasource.partitions.parquet

import com.vertica.spark.datasource.partitions.mixin.Identifiable

/**
 * Represents a portion of a parquet file
 *
 * @param filename    Full path with name of the parquet file
 * @param minRowGroup First row group to read from parquet file
 * @param maxRowGroup Last row group to read from parquet file
 * @param rangeIdx    Range index for this file. Used to track access to this file / cleanup among different nodes.
 *                    If there are three ranges for a given file this will be a value between 0 and 2
 */
final case class ParquetFileRange(filename: String, minRowGroup: Int, maxRowGroup: Int, rangeIdx: Int) extends Identifiable {

  override def index: Long = this.rangeIdx
}
