CONNECTOR_VERSION=$(cat ../../version.properties | grep ${connector-version} | cut -d'=' -f2)
export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.0/bin/hadoop classpath)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

export SPARK_MASTER_HOST=docker-client-1
start-master.sh