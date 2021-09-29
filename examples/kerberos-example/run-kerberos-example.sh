export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.1/bin/hadoop classpath)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
start-master.sh
start-worker.sh spark://localhost:7077
if [ "$1" == "debug" ]
  then
    export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5050
fi
spark-submit --master spark://localhost:7077 --conf "spark.driver.extraClassPath={$SPARK_HOME}/conf/" --driver-java-options '-Djava.security.auth.login.config=/spark-connector/docker/client-krb/jaas.config' ./target/scala-2.12/spark-vertica-connector-kerberos-example-assembly-2.0.3.jar
