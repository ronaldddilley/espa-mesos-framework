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

jobfile='jobs.json'

#espa-framework/espa-framework.py --job-filename ${jobfile} ${@}

docker run --rm \
    --volume ${HOME}/.usgs/espa:/root/.usgs/espa \
    --volume ${HOME}/job-dir:/job-dir \
    --workdir /job-dir \
    --name mesos_framework \
    espa/framework:latest \
    espa-framework.py --job-filename ${jobfile}

