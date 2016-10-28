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
#     Assumes everything is running on the same system.
#-----------------------------------------------------------------------------

set -e

use_docker_containerizer='yes'
port=${1}
resources=${2}
zookeeper=${3}
sbin_path='/home/dilley/dev-tools/sbin'
work_dir='/data2/dilley/mesos/'${port}
agent_log='/data2/dilley/mesos/agent-'${port}'.log'

containerize=''
docker_remove_delay=''
if [ ${use_docker_containerizer} == 'yes' ]; then
    echo '*** Agent will use docker containerizer ***'
    containerize='--containerizers=docker'
    docker_remove_delay='--docker_remove_delay=1mins'
fi

sudo rm -f ${work_dir}/meta/slaves/latest

sudo nohup ${sbin_path}/mesos-agent \
    --master=${zookeeper} \
    --port=${port} \
    --work_dir=${work_dir} \
    --external_log_file=${agent_log} \
    --resources=${resources} \
    ${containerize} ${docker_remove_delay} \
    </dev/null >/dev/null 2>&1 &
