FROM eclipse-temurin:11.0.16.1_1-jdk-jammy

ARG SPARK=3.3.0

# Install SBT
RUN apt-get update && \
    apt-get -y install apt-transport-https curl gnupg -yqq && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
    chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg && \
    apt-get update && \
    apt-get -y install sbt

# Install Spark
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK}/spark-${SPARK}-bin-without-hadoop.tgz && \
    wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.2/hadoop-3.3.2.tar.gz && \
    tar xvf spark-${SPARK}-bin-without-hadoop.tgz && \
    tar xvf hadoop-3.3.2.tar.gz && \
    mv spark-${SPARK}-bin-without-hadoop/ /opt/spark && \
    cd /opt/spark/conf && \
    mv spark-env.sh.template spark-env.sh

ENV SPARK_HOME="/opt/spark"   
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$HADOOP_HOME/bin:$PATH"
RUN echo "export SPARK_DIST_CLASSPATH=$(/hadoop-3.3.2/bin/hadoop classpath)" >> /opt/spark/conf/spark-env.sh
