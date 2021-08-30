wget -P ../../ https://archive.apache.org/dist/spark/spark-3.0.3/spark-3.0.3-bin-without-hadoop.tgz
wget -P ../../ https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz
cd ../../
tar xvf spark-3.0.3-bin-without-hadoop.tgz
tar xvf hadoop-3.3.0.tar.gz
mv spark-3.0.3-bin-without-hadoop/ /opt/spark
cd /opt/spark/conf
mv spark-env.sh.template spark-env.sh
export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.0/bin/hadoop classpath)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
start-master.sh
start-slave.sh spark://localhost:7077
cd ../../../spark-connector/functional-tests
spark-submit --master spark://localhost:7077 target/scala-2.12/spark-vertica-connector-functional-tests-assembly-2.0.2.jar
