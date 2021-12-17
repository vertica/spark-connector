FROM centos:8

 RUN (cd /lib/systemd/system/sysinit.target.wants/; for i in *; do [ $i == \
 systemd-tmpfiles-setup.service ] || rm -f $i; done); \
 rm -f /lib/systemd/system/multi-user.target.wants/*;\
 rm -f /etc/systemd/system/*.wants/*;\
 rm -f /lib/systemd/system/local-fs.target.wants/*; \
 rm -f /lib/systemd/system/sockets.target.wants/*udev*; \
 rm -f /lib/systemd/system/sockets.target.wants/*initctl*; \
 rm -f /lib/systemd/system/basic.target.wants/*;\
 rm -f /lib/systemd/system/anaconda.target.wants/*;
 RUN yum install python2 wget -y
 RUN  wget https://raw.githubusercontent.com/gdraheim/docker-systemctl-replacement/master/files/docker/systemctl.py -O /usr/local/bin/systemctl
 RUN chmod a+x /usr/local/bin/systemctl
 RUN yum -y install initscripts && yum clean all
 RUN yum install krb5-server krb5-libs krb5-workstation -y
 EXPOSE 88

 CMD ["/usr/sbin/init"]
