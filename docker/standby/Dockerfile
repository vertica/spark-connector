FROM openjdk:8
MAINTAINER vertica

ENV DEBIAN_FRONTEND noninteractive

# Refresh package lists
RUN apt-get update
RUN apt-get -qy dist-upgrade

RUN apt-get install -qy rsync curl openssh-server openssh-client vim nfs-common
RUN apt -y install krb5-kdc krb5-admin-server krb5-config

RUN mkdir -p /data/hdfs-nfs/
RUN mkdir -p /opt
WORKDIR /opt

# Install Hadoop
RUN curl -L https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz -s -o - | tar -xzf -
RUN mv hadoop-3.3.1 hadoop
RUN apt -y install krb5-kdc krb5-admin-server krb5-config

# Setup
WORKDIR /opt/hadoop
ENV PATH /opt/hadoop/bin:/opt/hadoop/sbin:$PATH
RUN echo $JAVA_HOME
ENV JAVA_HOME /usr/local/openjdk-8
RUN sed --in-place='.ori' -e "s/\${JAVA_HOME}/\/usr\/local\/openjdk-8/" etc/hadoop/hadoop-env.sh

# Configure ssh client
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

RUN echo "\nHost *\n" >> ~/.ssh/config && \
    echo "   StrictHostKeyChecking no\n" >> ~/.ssh/config && \
    echo "   UserKnownHostsFile=/dev/null\n" >> ~/.ssh/config

# Disable sshd authentication
RUN echo "root:root" | chpasswd
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/#PubkeyAuthentication yes/PubkeyAuthentication yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

# Pseudo-Distributed Operation
RUN echo "export JAVA_HOME=/usr/local/openjdk-8" >> /opt/hadoop/etc/hadoop/hadoop-env.sh
#RUN hdfs namenode -bootstrapStandby && \
#hdfs -initializeSharedEdits

ENV HDFS_NAMENODE_USER root
ENV HDFS_DATANODE_USER root

# SSH
EXPOSE 22
# hdfs://localhost:8020
EXPOSE 8020
EXPOSE 8022
EXPOSE 9870
EXPOSE 9871

ENTRYPOINT hadoop-daemon.sh start portmap \
  && hadoop-daemon.sh start nfs3 \
  && bash || bash