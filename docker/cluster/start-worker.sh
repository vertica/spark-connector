export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.0/bin/hadoop classpath)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

start-worker.sh spark://docker-client-1:7077
