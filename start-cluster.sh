#! /usr/bin/bash

#-----------------------------------------------------------------------------
# File:
#     start-cluster
#
# Purpose:
#     Starts a Mesos cluster on the current system.
#
# License:
#     NASA Open Source Agreement 1.3
#
# Notes:
#     Assumes everything is running on the same system.
#-----------------------------------------------------------------------------

set -e

# Start Zookeepers
cd zookeeper-3.4.9-server-1
./bin/zkServer.sh start
cd ..
sleep 2

cd zookeeper-3.4.9-server-2
./bin/zkServer.sh start
cd ..
sleep 2

cd zookeeper-3.4.9-server-3
./bin/zkServer.sh start
cd ..
sleep 5

# Start Mesos Masters
echo "Starting Mesos Master-1"
./start-mesos-master.sh 5050
sleep 2

echo "Starting Mesos Master-2"
./start-mesos-master.sh 5060
sleep 2

echo "Starting Mesos Master-3"
./start-mesos-master.sh 5070
sleep 5

# Start Mesos Agents
echo "Starting Mesos Agent-1"
./start-mesos-agent.sh 5051
sleep 2

echo "Starting Mesos Agent-2"
./start-mesos-agent.sh 5061 &> agent-2.log
sleep 2

echo "Starting Mesos Agent-3"
./start-mesos-agent.sh 5071 &> agent-3.log
sleep 2

echo "Cluster is probably up ????"
