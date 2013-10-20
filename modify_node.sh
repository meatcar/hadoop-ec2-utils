#!/bin/bash
DATASTORE='/home/hduser/hadoop/datastore'
HADOOP_BIN='/home/hduser/hadoop/hadoop-1.2.1/bin'

sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo echo \"$2\" >> /home/hduser/authorized_keys'"
sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo mkfs.ext3 /dev/$3; sudo mount /dev/$3 $DATASTORE; sudo chown hduser:hadoop $DATASTORE'"
sudo -u ubuntu sh -c "ssh -o 'StrictHostKeyChecking no' -i hadoopKey.pem $1 -t 'sudo -u hduser sh -c \"sh $HADOOP_BIN/hadoop datanode & $HADOOP_BIN/hadoop tasktracker\"'"
