FROM centos:7

ENV SBT_VERSION 1.3.13
#ENV JAVA_OPTS="$JAVA_OPTS -Djava.security.auth.login.config=/spark-connector/conf/jaas.config"

RUN yum install -y java-11-openjdk && \
    yum install -y epel-release && \
    yum update -y && yum install -y wget && \
    curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo && \
    mv sbt-rpm.repo /etc/yum.repos.d/ && \
    yum -y install sbt

ENTRYPOINT ["/bin/bash"]
