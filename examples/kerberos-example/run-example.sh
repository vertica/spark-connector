export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.0/bin/hadoop classpath)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
start-master.sh
start-worker.sh spark://localhost:7077
spark-submit --master spark://localhost:7077 --conf "spark.driver.extraClassPath={$SPARK_HOME}/conf/" --driver-java-options '-Djava.security.auth.login.config=/spark-connector/docker/client-krb/jaas.config' $1
