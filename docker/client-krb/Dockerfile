FROM centos:7
ENV SBT_VERSION 1.3.13
ENV JAVA_OPTS="$JAVA_OPTS -Djava.security.auth.login.config=/spark-connector/docker/client-krb/jaas.config"

RUN yum install -y java-11-openjdk && \
    yum install -y krb5-workstation && \
    yum install -y epel-release && \
    yum update -y && yum install -y wget && \
    curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo && \
    mv sbt-rpm.repo /etc/yum.repos.d/ && \
    yum -y install sbt && \
    wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-without-hadoop.tgz && \
    wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz && \
    tar xvf spark-3.1.2-bin-without-hadoop.tgz && \
    tar xvf hadoop-3.3.1.tar.gz && \
    yum install -y openssh-clients openssh-server && \
    yum install -y nano && \
    mv spark-3.1.2-bin-without-hadoop/ /opt/spark && \
    cd /opt/spark/conf && \
    mv spark-env.sh.template spark-env.sh && \
    ssh-keygen -t rsa -P "" -f /root/.ssh/id_rsa && \
    cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

ENV PATH "$PATH:opt/spark/bin:opt/spark/sbin"

ENTRYPOINT ["/usr/sbin/init"]