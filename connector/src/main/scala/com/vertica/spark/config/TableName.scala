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

object EscapeUtils {
  def sqlEscape(str: String, char: Char = '\"'): String = {
    val c = char.toString
    str.replace(c, c + c)
  }

  def sqlEscapeAndQuote(str: String): String = {
    "\"" + sqlEscape(str) + "\""
  }
}

/**
 * Parent trait representing a set of data being read from
 */
trait TableSource {
  /**
   * Get a unique identifier for the operation.
   *
   * This value is used in a filepath.
   */
  def identifier : String
}

/**
 * Represents a fully qualified tablename in Vertica.
 *
 * @param name Name of the table
 * @param dbschema Optionally, the schema of the table. Public schema will be assumed if not specified.
 */
final case class TableName(name: String, dbschema: Option[String]) extends TableSource {

  /**
   * Returns the full name of the table, escaped and surrounded with double quotes to prevent injection
   * and allow for special characters.
   */
  def getFullTableName : String = {
    dbschema match {
      case None => EscapeUtils.sqlEscapeAndQuote(name)
      case Some(schema) => EscapeUtils.sqlEscapeAndQuote(schema) + "." + EscapeUtils.sqlEscapeAndQuote(name)
    }
  }

  def getTableName : String = EscapeUtils.sqlEscapeAndQuote(name)

  def getDbSchema : String =  {
    dbschema match {
      case None => ""
      case Some(schema) => EscapeUtils.sqlEscapeAndQuote(schema)
    }
  }

  /**
   * The table's name is used as an identifier for the operation.
   */
  override def identifier: String = name
}

final case class TableQuery(query: String, uniqueId: String, dbSchema: Option[String]) extends TableSource {
  override def identifier: String = uniqueId
}

