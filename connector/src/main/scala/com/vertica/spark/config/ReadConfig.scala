package com.vertica.spark.config

import com.typesafe.scalalogging.Logger

sealed trait ReadConfig

case class HDFSMethodReadConfig(logger: Logger) extends ReadConfig
