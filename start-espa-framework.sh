#! /usr/bin/bash

#-----------------------------------------------------------------------------
# File:
#     start-espa-framework
#
# Purpose:
#     Starts the ESPA framework on the current system.
#
# License:
#     NASA Open Source Agreement 1.3
#
# Notes:
#     Assumes everything is running on the same system.
#-----------------------------------------------------------------------------

set -e

zookeeper='zk://localhost:2181,localhost:2182,localhost:2183/mesos'
jobfile='jobs.json'

./espa-framework.py \
    --master-node ${zookeeper} \
    --job-filename ${jobfile}
