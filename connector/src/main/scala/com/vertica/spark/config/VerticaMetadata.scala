package com.vertica.spark.config

import org.apache.spark.sql.types._

case class VerticaMetadata(val schema: StructType)
