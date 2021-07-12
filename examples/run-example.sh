if [ "$1" == "clean" ]
  then
    wget -P ../../ https://mirror.dsrg.utoronto.ca/apache/spark/spark-3.1.2/spark-3.1.2-bin-without-hadoop.tgz
    wget -P ../../ https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz
    cd ../../
    tar xvf spark-3.1.2-bin-without-hadoop.tgz
    tar xvf hadoop-3.3.0.tar.gz
    mv spark-3.1.2-bin-without-hadoop/ /opt/spark
    cd /opt/spark/conf
    mv spark-env.sh.template spark-env.sh
    export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
    export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.0/bin/hadoop classpath)
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
    start-master.sh
    start-slave.sh spark://localhost:7077
    cd ../../../spark-connector/examples
    spark-submit --master spark://localhost:7077 $2

elif [ -a ../../hadoop-3.3.0 ]
  then
    export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
    export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.0/bin/hadoop classpath)
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
    spark-submit --master spark://localhost:7077 $1

else
  wget -P ../../ https://mirror.dsrg.utoronto.ca/apache/spark/spark-3.1.2/spark-3.1.2-bin-without-hadoop.tgz
  wget -P ../../ https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz
  cd ../../
  tar xvf spark-3.1.2-bin-without-hadoop.tgz
  tar xvf hadoop-3.3.0.tar.gz
  mv spark-3.1.2-bin-without-hadoop/ /opt/spark
  cd /opt/spark/conf
  mv spark-env.sh.template spark-env.sh
  export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
  export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.0/bin/hadoop classpath)
  export SPARK_HOME=/opt/spark
  export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
  start-master.sh
  start-slave.sh spark://localhost:7077
  cd ../../../spark-connector/examples
  spark-submit --master spark://localhost:7077 $1

fi