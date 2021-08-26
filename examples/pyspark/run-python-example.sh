yum install -y python3
export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.0/bin/hadoop classpath)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
start-master.sh
start-worker.sh spark://localhost:7077
spark-submit --jars ../../connector/target/scala-2.12/spark-vertica-connector-assembly-2.0.1.jar sparkapp.py




