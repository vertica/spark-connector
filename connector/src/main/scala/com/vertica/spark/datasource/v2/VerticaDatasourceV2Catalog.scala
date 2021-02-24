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

package com.vertica.spark.datasource.v2

import java.util

import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import collection.JavaConverters._

final case class NoCatalogException(private val message: String = "Catalog functionality not implemented for Vertica source.")
  extends Exception(message)

/**
 * Implementation of the V2 catalog API for Vertica.
 *
 * This is not currently a full catalog implementation. This simply returns a table object upon request,
 * and indicates whether the table already exists. This is in place to allow for the Ignore and ErrorIfExist
 * save modes.
 */
class VerticaDatasourceV2Catalog extends TableCatalog{


  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    println("CATALOG OPTIONS: ")
    options.asScala.toMap.foreach(p => println(">>> key=" + p._1 + ", value=" + p._2))
    VerticaDatasourceV2Catalog.catalogOptions = Some(options)
  }

  override def tableExists(ident: Identifier): Boolean = {
    val opt = VerticaDatasourceV2Catalog.getOptions.getOrElse(throw new NoSuchTableException(ident))

    val table = new VerticaTable(opt)
    val schema = table.schema()

    schema.nonEmpty
  }

  override def name: String = "VerticaCatalog"

  @throws[NoSuchNamespaceException]
  override def listTables(namespace: Array[String]): Array[Identifier] = ???

  @throws[NoSuchTableException]
  override def loadTable(ident: Identifier): Table = {
    val opt = VerticaDatasourceV2Catalog.getOptions.getOrElse(throw new NoSuchTableException(ident))

    println("Loading table with OPTIONS: ")
    opt.asScala.toMap.foreach(p => println(">>> key=" + p._1 + ", value=" + p._2))

    new VerticaTable(opt)
  }

  @throws[TableAlreadyExistsException]
  @throws[NoSuchNamespaceException]
  def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
    loadTable(ident)
  }


  @throws[NoSuchTableException]
  def alterTable(ident: Identifier, changes: TableChange*): Table = throw new NoCatalogException

  def dropTable(ident: Identifier): Boolean = throw new NoCatalogException


  @throws[NoSuchTableException]
  @throws[TableAlreadyExistsException]
  def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = throw new NoCatalogException
}

/**
 * Stores options. Passed in from datasource. Options may also be specified under spark.sql.catalog.vertica.[option]
 */
object VerticaDatasourceV2Catalog {
  private var catalogOptions: Option[CaseInsensitiveStringMap] = None
  private var operationOptions: Option[CaseInsensitiveStringMap] = None

  def setOptions(opts: CaseInsensitiveStringMap) = {
    this.operationOptions = Some(opts)
  }

  def getOptions: Option[CaseInsensitiveStringMap] = {
    (catalogOptions, operationOptions) match {
      case (Some(cat), Some(op)) =>
        val m = new util.HashMap[String, String]()
        m.putAll(cat)
        m.putAll(op)
        Some(new CaseInsensitiveStringMap(cat))
      case (Some(cat), None) => Some(cat)
      case (None, Some(op)) => Some(op)
      case (None, None) => None
    }
  }
}