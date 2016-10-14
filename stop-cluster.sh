#! /usr/bin/bash

#-----------------------------------------------------------------------------
# File:
#     stop-cluster
#
# Purpose:
#     Stops the Mesos cluster on the current system.
#
# License:
#     NASA Open Source Agreement 1.3
#
# Notes:
#     Assumes everything is running on the same system.
#-----------------------------------------------------------------------------

set -e

# Stop Mesos Agents
echo "Stopping Mesos Agents"
sudo pkill mesos-agent

# Stop Mesos Masters
echo "Stopping Mesos Masters"
pkill mesos-master

# Stop Zookeepers
cd zookeeper-3.4.9-server-1
./bin/zkServer.sh stop
cd ..
sleep 2

cd zookeeper-3.4.9-server-2
./bin/zkServer.sh stop
cd ..
sleep 2

cd zookeeper-3.4.9-server-3
./bin/zkServer.sh stop
cd ..


