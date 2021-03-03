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

package com.vertica.spark.datasource

import com.vertica.spark.datasource.v2._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import java.util

import org.apache.spark.sql.SparkSession

import collection.JavaConverters._


/**
 * Entry-Point for Spark V2 Datasource.
 *
 * Implements Spark V2 datasource class [[http://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/catalog/TableProvider.html here]]
 *
 * This and the tree of classes returned by is are to be kept light, and hook into the core of the connector
 */
class VerticaSource extends TableProvider with SupportsCatalogOptions {

  /**
   * Used for read operation to get the schema for the table being read from
   *
   * @param caseInsensitiveStringMap A string map of options that was passed in by user to datasource
   * @return The table's schema in spark StructType format
   */
  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    val table = getTable(schema = null, partitioning = Array.empty[Transform], properties = caseInsensitiveStringMap)
    table.schema()
  }

  /**
   * Gets the structure representing a Vertica table
   *
   * @param schema       StructType representing table schema, used for write
   * @param partitioning specified partitioning for the table
   * @param properties   A string map of options that was passed in by user to datasource
   * @return [[VerticaTable]]
   */
  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table = {
    new VerticaTable(new CaseInsensitiveStringMap(properties))
  }


  override def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = {
    val name = options.asScala.toMap.getOrElse("table", "")
    Identifier.of(Array[String](), name)
  }

  private val CATALOG_NAME = "vertica"
  override def extractCatalog(options: CaseInsensitiveStringMap): String = {
    println("EXTRACT CATALOG OPTIONS: ")
    options.asScala.toMap.foreach(p => println(">>> key=" + p._1 + ", value=" + p._2))

    // Set the spark conf for catalog class
    SparkSession.getActiveSession.get.conf.set("spark.sql.catalog." + CATALOG_NAME, "com.vertica.spark.datasource.v2.VerticaDatasourceV2Catalog")

    // Add all passed in options to spark catalog options
    VerticaDatasourceV2Catalog.setOptions(options)

    CATALOG_NAME
  }

}

