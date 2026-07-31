#!/bin/bash
set -e

case "$SPARK_MODE" in
  master)
    exec "$SPARK_HOME/sbin/start-master.sh" --no-daemonize
    ;;
  worker)
    exec "$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER_URL" --no-daemonize
    ;;
  *)
    exec "$@"
    ;;
esac
