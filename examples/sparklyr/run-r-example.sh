yum install -y epel-release
yum install -y R
yum install -y openssl-devel
yum install -y libxml2-devel
yum install -y libcurl-devel
export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.0/bin/hadoop classpath)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
start-master.sh
start-slave.sh spark://localhost:7077
Rscript run.r
