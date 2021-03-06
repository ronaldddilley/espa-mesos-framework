FROM centos:7.3.1611


MAINTAINER USGS EROS LSRD http://eros.usgs.gov


LABEL description="(Processing Architecture) Mesos Framework"


# Update the OS packages to the latest versions
RUN yum update -y \
    && yum clean all


# Install the epel so we can get some good stuff
RUN yum install -y \
        epel-release \
    && yum clean all


# Initial Python Configuration
RUN yum install -y \
        python-pip \
        python-devel \
    && yum clean all


# Update pip
RUN pip install --upgrade pip


# The latest versions of Mesos require svn development package >= 1.9
COPY wandisco-svn.repo /etc/yum.repos.d/wandisco-svn.repo

# Install the mesosphere repo and install mesos from that repo
RUN yum -y install http://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-3.noarch.rpm \
    && yum install -y mesos \
    && yum clean all


# Install python access to the protobuf
RUN pip install protobuf requests


# Set the Mesos SSL environment variables.
#ENV LIBPROCESS_SSL_ENABLED=1 \
#    LIBPROCESS_SSL_VERIFY_CERT=0 \
#    LIBPROCESS_SSL_CERT_FILE="/etc/mesos/mesos_certpack/mesos.crt" \
#    LIBPROCESS_SSL_KEY_FILE="/etc/mesos/mesos_certpack/mesos.key" \
#    LIBPROCESS_SSL_CA_DIR="/etc/mesos/mesos_certpack" \
#    LIBPROCESS_SSL_ENABLE_SSL_V3=0 \
#    LIBPROCESS_SSL_ENABLE_TLS_V1_0=0 \
#    LIBPROCESS_SSL_ENABLE_TLS_V1_1=0 \
#    LIBPROCESS_SSL_ENABLE_TLS_V1_2=1


COPY config_utils.py /usr/bin/
COPY espa_base_exception.py /usr/bin/
COPY espa-framework.py /usr/bin/
