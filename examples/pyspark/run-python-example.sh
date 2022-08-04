CONNECTOR_VERSION=$(cat ../../version.properties | grep ${connector-version} | cut -d'=' -f2)
spark-submit --master spark://spark:7077 --jars ../../connector/target/scala-2.12/spark-vertica-connector-assembly-CONNECTOR_VERSION.jar sparkapp.py




