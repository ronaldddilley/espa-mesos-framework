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

use_docker_containerizer='yes'
if [ ${1} ]; then
    use_docker_containerizer=${1}
fi

master_node=`hostname --ip-address`
master_port=5050
sbin_path='/home/dilley/dev-tools/sbin'
work_dir='/data2/dilley/mesos'
agent_log='/data2/dilley/mesos/agent.log'
resources='cpus(*):3; mem(*):6000; disk(*):20000'

containerize=''
docker_remove_delay=''
if [ ${use_docker_containerizer} == 'yes' ]; then
    echo '*** Agent will use docker containerizer ***'
    containerize='--containerizers=docker'
    docker_remove_delay='--docker_remove_delay=1mins'
fi

sudo rm -f ${work_dir}/meta/slaves/latest

sudo ${sbin_path}/mesos-agent \
    --master=${master_node}:${master_port} \
    --work_dir=${work_dir} \
    --external_log_file=${agent_log} \
    --resources=${resources} \
    ${containerize} ${docker_remove_delay}
