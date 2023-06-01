#!/usr/bin/env bash

# Append option -r to the list of args
args=("-r")
args+=("$@")

spark-submit --master spark://spark:7077 --driver-memory 2g target/scala-2.12/vertica-spark-functional-tests.jar "${args[@]}"
