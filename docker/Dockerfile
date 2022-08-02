ARG SPARK=latest
FROM bitnami/spark:$SPARK
USER root

RUN apt-get update
RUN apt-get install -y sudo

# Install JDK.
RUN sudo apt-get install -y openjdk-11-jdk
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
# Prepending our JAVA_HOME so that it would take precedent over bitnami's java home path.
ENV PATH=$JAVA_HOME/bin:$PATH

# Install SBT
RUN sudo apt-get update && \
    sudo apt-get -y install apt-transport-https curl gnupg -yqq && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
    sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg && \
    sudo apt-get update && \
    sudo apt-get -y install sbt