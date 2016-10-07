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
#-----------------------------------------------------------------------------

set -e

cluster_name='Warlock'
master_node=`hostname --ip-address`
sbin_path='/home/dilley/dev-tools/sbin'
work_dir='/data2/dilley/mesos'
master_log='/data2/dilley/mesos/master.log'

${sbin_path}/mesos-master \
    --ip=${master_node} \
    --cluster=${cluster_name} \
    --work_dir=${work_dir} \
    --external_log_file=${master_log}
