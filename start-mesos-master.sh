#! /usr/bin/bash

#-----------------------------------------------------------------------------
# File:
#     start-mesos-master
#
# Purpose:
#     Starts a Mesos master on the current system.
#
# License:
#     NASA Open Source Agreement 1.3
#
# Notes:
#     Assumes everything is running on the same system.
#-----------------------------------------------------------------------------

set -e

cluster_name=${1}
port=${2}
zookeeper=${3}

master_node=`hostname --ip-address`
sbin_path='/home/dilley/dev-tools/sbin'
work_dir='/data2/dilley/mesos/'${port}
master_log='/data2/dilley/mesos/master-'${port}'.log'

nohup ${sbin_path}/mesos-master \
    --port=${port} \
    --cluster=${cluster_name} \
    --work_dir=${work_dir} \
    --external_log_file=${master_log} \
    --quorum=1 \
    --zk=${zookeeper} \
    </dev/null >/dev/null 2>&1 &
