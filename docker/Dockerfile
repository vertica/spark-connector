FROM centos:7

ENV SBT_VERSION 1.3.13
#ENV JAVA_OPTS="$JAVA_OPTS -Djava.security.auth.login.config=/spark-connector/conf/jaas.config"

RUN yum install -y java-11-openjdk && \
    yum install -y epel-release && \
    yum update -y && yum install -y wget && \
    wget https://github.com/sbt/sbt/releases/download/v1.3.13/sbt-1.3.13.tgz && \
    tar xvf sbt-1.3.13.tgz

ENTRYPOINT ["/bin/bash"]