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

package com.vertica.spark.config

import org.apache.spark.sql.types._

/**
 * Abstract trait for passing metadata of a table retrieved from Vertica.
 */
trait VerticaMetadata

/**
 * Metadata for read operation.
 * @param schema Schema of the table being read in Vertica.
 */
final case class VerticaReadMetadata(schema: StructType) extends VerticaMetadata

/**
 * Empty class; No metadata retrieval required for current write operation.
 */
final case class VerticaWriteMetadata() extends VerticaMetadata

