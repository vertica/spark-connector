FROM openjdk:8
MAINTAINER vertica

ENV DEBIAN_FRONTEND noninteractive

# Refresh package lists
RUN apt-get update
RUN apt-get -qy dist-upgrade

RUN apt-get install -qy rsync curl openssh-server openssh-client vim nfs-common

RUN mkdir -p /data/hdfs-nfs/
RUN mkdir -p /opt
WORKDIR /opt

# Install Hadoop
RUN curl -L https://dlcdn.apache.org/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz -s -o - | tar -xzf -
RUN mv hadoop-2.10.1 hadoop

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
#RUN sed -i 's/127.0.0.1       localhost/127.0.0.1       localhost hdfs hdfs.example.com/' /etc/hosts

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
# => Quick fix for enabling datanode connections
#    ssh -L 50010:localhost:50010 root@192.168.99.100 -p 22022 -o PreferredAuthentications=password

# Pseudo-Distributed Operation
COPY ./hadoop/core-site.xml etc/hadoop/core-site.xml
COPY ./hadoop/hdfs-site.xml etc/hadoop/hdfs-site.xml
RUN hdfs namenode -format


# SSH
EXPOSE 22
# hdfs://localhost:8020
EXPOSE 8020
# HDFS namenode
EXPOSE 50020
# HDFS Web browser
EXPOSE 50070
# HDFS datanodes
EXPOSE 50075
# HDFS secondary namenode
EXPOSE 50090

ENTRYPOINT service ssh start \
  && start-dfs.sh \
  && hadoop-daemon.sh start portmap \
  && hadoop-daemon.sh start nfs3 \
  && bash || bash