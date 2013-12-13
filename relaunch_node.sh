#!/bin/bash
DATASTORE='/home/hduser/hadoop/datastore'
HADOOP_BIN='/home/hduser/hadoop/hadoop-1.2.1/bin'
SSHDIR="/home/hduser/.ssh"

# CHECK OUT ALTERNATIVES: CHEFS, PUPPET, VAGRANT, ANSIBLE. REMOTE MACHINE SET UPS IS A PROBLEM THAT HAS BEEN SOLVED ALREADY

# copies the master's ssh key into the slave.
sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo -u hduser sh -c \" echo $2 > $SSHDIR/authorized_keys \"'"

# Mounts drive, changes ownership to hadoop group and user hduser. 
sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo mount /dev/$3 $DATASTORE; sudo chown hduser:hadoop $DATASTORE'"

# update the MASTER mapping
sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo sed -i \"s/.*master/$4 master/g\" /etc/hosts'"

# Starts the datanode and the tasktracker.
sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo -u hduser sh -c \"bash $HADOOP_BIN/hadoop-daemon.sh start datanode\"'"
sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo -u hduser sh -c \"bash $HADOOP_BIN/hadoop-daemon.sh start tasktracker\"'"

