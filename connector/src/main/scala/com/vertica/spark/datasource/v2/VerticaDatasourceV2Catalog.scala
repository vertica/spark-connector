package com.vertica.spark.datasource.v2

import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import collection.JavaConverters._

class VerticaDatasourceV2Catalog extends TableCatalog{

  var options: Option[CaseInsensitiveStringMap] = None

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    println("CATALOG OPTIONS: ")
    options.asScala.toMap.foreach(p => println(">>> key=" + p._1 + ", value=" + p._2))
    this.options = Some(options)
  }

  override def tableExists(ident: Identifier): Boolean = {
    val opt = options.getOrElse(throw new NoSuchTableException(ident))

    val table = new VerticaTable(opt)
    val schema = table.schema()

    schema.nonEmpty
  }

  override def name: String = "VerticaCatalog"

  @throws[NoSuchNamespaceException]
  override def listTables(namespace: Array[String]): Array[Identifier] = ???


  @throws[NoSuchTableException]
  override def loadTable(ident: Identifier): Table = {
    val opt = options.getOrElse(throw new NoSuchTableException(ident))

    new VerticaTable(opt)
  }

  @throws[TableAlreadyExistsException]
  @throws[NoSuchNamespaceException]
  def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
    loadTable(ident)
  }


  @throws[NoSuchTableException]
  def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  def dropTable(ident: Identifier): Boolean = ???


  @throws[NoSuchTableException]
  @throws[TableAlreadyExistsException]
  def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???
}
