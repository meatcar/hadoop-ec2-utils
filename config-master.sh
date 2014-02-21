#!/bin/bash

HADOOP_DIR="/home/hduser/hadoop/hadoop-1.2.1"

if [ ! "$USER" = "root" ]; then
	echo "Must run the script as root">&2
	exit 1
fi

# first, get the IP of the master
master_ip="$(hostname | sed s/ip\-// | sed s/\-/./g)"

# change the master entry in the hosts file
sed -i "s/[0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\+ master/$master_ip master/" /etc/hosts

# change the conf master and slaves files in hadoop directory
pushd "$HADOOP_DIR">/dev/null
echo "$master_ip" > conf/masters
echo "$master_ip" > conf/slaves

echo "Done configuring master"
