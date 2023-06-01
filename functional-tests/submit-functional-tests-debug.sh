#!/usr/bin/env bash

export SPARK_SUBMIT_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005"

./submit-functional-tests.sh "$@"
