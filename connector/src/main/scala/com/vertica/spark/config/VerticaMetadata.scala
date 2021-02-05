package com.vertica.spark.config

import org.apache.spark.sql.types._

trait VerticaMetadata
final case class VerticaReadMetadata(schema: StructType) extends VerticaMetadata
final case class VerticaWriteMetadata() extends VerticaMetadata

