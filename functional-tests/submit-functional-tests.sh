CONNECTOR_VERSION=$(cat ../version.properties | grep ${connector-version} | cut -d'=' -f2)
export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.0/bin/hadoop classpath)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
start-master.sh -h localhost
start-worker.sh -h localhost spark://localhost:7077
spark-submit --master spark://localhost:7077 target/scala-2.12/spark-vertica-connector-functional-tests-assembly-$CONNECTOR_VERSION.jar $1