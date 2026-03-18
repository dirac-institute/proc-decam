FROM lsstsqre/centos:7-stack-lsst_distrib-w_2024_34

USER root

# Install PostgreSQL server and client tools
RUN yum install -y postgresql-server postgresql-contrib && yum clean all

# Revert to the default LSST stack user so the image does not run as root by default
USER lsst
WORKDIR /home/lsst
