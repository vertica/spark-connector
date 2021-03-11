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

/**
 * Represents a fully qualified tablename in Vertica.
 *
 * @param name Name of the table
 * @param dbschema Optionally, the schema of the table. Public schema will be assumed if not specified.
 */
final case class TableName(name: String, dbschema: Option[String]) {

  /**
   * Returns the full name of the table, escaped and surrounded with double quotes to prevent injection
   * and allow for special characters.
   */
  def getFullTableName : String = {
    dbschema match {
      case None => "\"" + name + "\""
      case Some(schema) => "\"" + schema + "\".\"" + name + "\""
    }
  }
}
