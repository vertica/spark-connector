CONNECTOR_VERSION=$(cat ../../version.properties | grep ${connector-version} | cut -d'=' -f2)
spark-submit --master spark://spark:7077 --jars ./spark-vertica-connector-all-$CONNECTOR_VERSION.jar sparkapp.py




