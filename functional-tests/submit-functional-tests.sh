CONNECTOR_VERSION=$(cat ../version.properties | grep ${connector-version} | cut -d'=' -f2)
#export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
#export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.0/bin/hadoop classpath)
#export SPARK_HOME=/opt/spark
#export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
#export HADOOP_HOME=/hadoop-3.3.0/
#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

#start-master.sh --host spark
#start-worker.sh spark://spark:7077
#spark-submit --master spark://spark-driver:7077 --driver-memory 2g target/scala-2.12/spark-vertica-connector-functional-tests_2.12-$CONNECTOR_VERSION.jar -r $1
spark-submit --master spark://spark:7077 --driver-memory 2g target/scala-2.12/spark-vertica-connector-functional-tests-assembly-$CONNECTOR_VERSION.jar -r $1