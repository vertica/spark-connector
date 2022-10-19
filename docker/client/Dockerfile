ARG SPARK=latest
FROM bitnami/spark:$SPARK

USER root

# Install JDK
RUN apt-get update && apt-get install -y openjdk-11-jdk
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
# Prepending our JAVA_HOME so that it would take precedent over bitnami's java home path.
ENV PATH=$JAVA_HOME/bin:$PATH

# Install SBT
RUN apt-get update && \
    apt-get -y install apt-transport-https curl gnupg -yqq && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
    chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg && \
    apt-get update && \
    apt-get -y install sbt

# Sync Python version
RUN curl https://raw.githubusercontent.com/bitnami/bitnami-docker-spark/master/3.3/debian-11/prebuildfs/opt/bitnami/scripts/libcomponent.sh -o /opt/bitnami/scripts/libcomponent.sh && \
    . /opt/bitnami/scripts/libcomponent.sh && component_unpack "python" "3.10.6-8" --checksum 84801ae4aee44f17012410d30a7d45e7e30ff842469fea2e5196050142713efc
