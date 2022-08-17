#!/usr/bin/env bash

spark-submit --master spark://spark:7077 --driver-memory 2g target/scala-2.12/vertica-spark-scala-examples.jar "$@"
