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

cluster_name='Warlock'

# Start Zookeepers
zk1_hp='localhost:2181'
cd zookeeper-3.4.9-server-1
./bin/zkServer.sh start
cd ..
sleep 2

zk2_hp='localhost:2182'
cd zookeeper-3.4.9-server-2
./bin/zkServer.sh start
cd ..
sleep 2

zk3_hp='localhost:2183'
cd zookeeper-3.4.9-server-3
./bin/zkServer.sh start
cd ..
sleep 5

# Zookeeper setup for mesos
zookeeper='zk://'${zk1_hp}','${zk2_hp}','${zk3_hp}'/mesos'

# Start Mesos Masters
port=5050
echo "Starting Mesos Master-1"
./start-mesos-master.sh ${cluster_name} ${port} ${zookeeper}
sleep 2

#echo "Starting Mesos Master-2"
#port=5060
#./start-mesos-master.sh ${cluster_name} ${port} ${zookeeper}
#sleep 2

#echo "Starting Mesos Master-3"
#port=5070
#./start-mesos-master.sh ${cluster_name} ${port} ${zookeeper}
#sleep 5

# Start Mesos Agents
port=5051
resources='cpus(*):3;mem(*):6000;disk(*):20000'
echo "Starting Mesos Agent-1"
./start-mesos-agent.sh ${port} ${resources} ${zookeeper}
sleep 2

#echo "Starting Mesos Agent-2"
#port=5061
#resources='cpus:1;mem:2000;disk:10000'
#./start-mesos-agent.sh ${port} ${resources} ${zookeeper}
#sleep 2
#
#echo "Starting Mesos Agent-3"
#port=5071
#resources='cpus:1;mem:2000;disk:10000'
#./start-mesos-agent.sh ${port} ${resources} ${zookeeper}
#sleep 2

echo "Cluster is probably up ????"
