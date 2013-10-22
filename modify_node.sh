#!/bin/bash
DATASTORE='/home/hduser/hadoop/datastore'
HADOOP_BIN='/home/hduser/hadoop/hadoop-1.2.1/bin'
COMMAND="echo \"$2\" >> /home/hduser/authorized_keys"

# copies the master's ssh key into the slave.
sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo -u hduser sh -c \"$COMMAND\" '"

# Formats the files system, changes ownership to hadoop group and user hduser. 
sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo mkfs.ext3 -j /dev/$3; sudo mount /dev/$3 $DATASTORE; sudo chown hduser:hadoop $DATASTORE'"

# Starts the datanode and the tasktracker.
sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo -u hduser sh -c \"sh $HADOOP_BIN/hadoop datanode & $HADOOP_BIN/hadoop tasktracker\"'"

# update the MASTER mapping
sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo sed -i \"s/.*master/$4 master/g\" /etc/hosts'"
