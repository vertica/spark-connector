ARG SPARK=4.1.1
ARG JAVA_VERSION=21

FROM eclipse-temurin:${JAVA_VERSION}-jdk-jammy

ARG SPARK

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV JAVA_HOME=/opt/java/openjdk

# Install Python, curl, gnupg for sbt
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl apt-transport-https gnupg && \
    rm -rf /var/lib/apt/lists/*

# Install sbt
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | \
    gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
    chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg && \
    apt-get update && apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK}/spark-${SPARK}-bin-hadoop3.tgz" | \
    tar -xz -C /opt && \
    mv /opt/spark-${SPARK}-bin-hadoop3 /opt/spark

COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["bash"]

