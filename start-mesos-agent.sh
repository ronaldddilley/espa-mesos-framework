#! /usr/bin/bash

#-----------------------------------------------------------------------------
# File:
#     start-mesos-agent.sh
#
# Purpose:
#     Starts a Mesos agent on the current system.
#
# License:
#     NASA Open Source Agreement 1.3
#
# Notes:
#     Assumes the master is running on the same system.
#-----------------------------------------------------------------------------

set -e

master_node=`hostname --ip-address`
master_port=5050
sbin_path='/home/dilley/dev-tools/sbin'
work_dir='/data2/dilley/mesos'
agent_log='/data2/dilley/mesos/agent.log'
resources='cpus(*):3; mem(*):6000; disk(*):20000'
#    --containerizers=docker

sudo rm -f ${work_dir}/meta/slaves/latest

sudo ${sbin_path}/mesos-agent \
    --master=${master_node}:${master_port} \
    --work_dir=${work_dir} \
    --external_log_file=${agent_log} \
    --resources=${resources}
