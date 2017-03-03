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

./espa-framework.py --job-filename ${jobfile}
