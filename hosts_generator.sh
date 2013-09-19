#!/bin/bash

cur_mip=`host ec2-54-211-94-69.compute-1.amazonaws.com | awk '{print $4}'`; 
cur_sip=`host ec2-107-22-124-88.compute-1.amazonaws.com | awk '{print $4}'`; 

master_ip="foo"
slave_ip="bar"

while [ true ]
do 
	
	cur_mip=`host ec2-54-211-94-69.compute-1.amazonaws.com | awk '{print $4}'`; 
	cur_sip=`host ec2-107-22-124-88.compute-1.amazonaws.com | awk '{print $4}'`; 

	if [ "$cur_mip" != "$master_ip" ]
	then
		master_ip=$cur_mip
		sed -i "s/.*master/$master_ip master/g" /etc/hosts
	fi

	if [ "$cur_sip" != "$slave_ip" ]
	then
		slave_ip=$cur_sip
		sed -i "s/.*slave/$slave_ip slave/g" /etc/hosts
	fi

	sleep 15
done

