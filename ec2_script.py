#!/usr/bin/python
# TODO:
# - Does AWS automagically remove dead instances?
# - no, it doesn't. remove (ie terminate it) whence done.

# When a node goes down: 
# 1. get the EBS id of the instance that is down.
# 2. start a new instance and attach the EBS from part 1.
# 3. Update the conf files in the master and update the host file in the master
#    with the ip of the slave and also remove the old slave which went down.
# 4. start data node process on the instance created in step 2.
# 5. update the data structure.
 
# Load the state of the data structure after connecting to EC2.
# 1. connect to EC2 and read all the instances.
# 2. for each instances, get the attached EBS (ie. the larger of the two EBS's)
# 3. operate under the assumption that
#    i. there are no dead nodes.
#    ii. all instances are part of the cluster.
# 4. Read the master from the configuration.


#------------------------------------------------------------------------------#
# Script for launch amazon EC2 instances and moving EBS devices around.        #
# author: Abdi Dahir, Karan Dhiman                                             #
# date: August 12th, 2013                                                      #
#                                                                              #
#------------------------------------------------------------------------------#
import boto
from time import sleep
import json
import os
from sys import exit
import subprocess
import threading
import pipes

from checkHeartbeat import run_once, LOOP_INTERVAL, DEAD_NODE_LIST

# State of the vm on ec2
ALIVE = True
DEAD = False

# Load in the ec2 properties located in the specified conf file
with open("conf.json") as conf:
    PROPERTIES = json.loads(conf.read())
AMI = PROPERTIES["ami"]
SECRET_KEY = PROPERTIES["secret_key"]
KEY = PROPERTIES["key"]
MASTER = PROPERTIES["master-ip"]

#REGION = PROPERTIES["region"]
with open("/home/hduser/.ssh/id_rsa.pub", "r") as sshkey:
    SSH_KEY = sshkey.read()
LOCK = threading.Lock()
conn = None
FREEZE=10

class instance_decorator:
    '''
    Datastructure for maintaining instances, their states and their respective 
    network block devices
    '''
    def __init__(self):
        # new plan:
        # Maintain four data structures within this object
        # 1 dictionary to keep track of instance_ids->ebs_id's
        # 1 list to keep track of all the alive instances.
        # 1 list to keep track of all the dead instances.
        # 1 dictionary to keep track of instance ip addresses to instance id's.
        
        # We assume that the EBS state is always fine.

        self.inst_to_ebs = dict()
        self.alive_instance_list = list()
        self.dead_instance_list = DEAD_NODE_LIST # need to detach the EBS from these.
        self.ip_to_inst = dict() 

        # add code to re construct the state of the data structure.
        
        # Algorithm:
        # Get all currently running instances with the 'prime*' tag
        #     for each of the instances get the 
        #     1. attached EBS (secondary).
        #     2. private IP
        #     3. the state - DEAD || ALIVE
        # populate the data structure with this information.
        
        all_instances = get_all_instances()
        for instance in all_instances:
            self.ip_to_inst[get_instance_private_ip(instance.id)] = instance.id
            self.inst_to_ebs[instance.id] = instance.block_device_mapping['/dev/sdz'].volume_id

            if instance.state == 'running':
                self.alive_instance_list.append(instance.id) 
            else:
                print "added a dead node"
                #self.dead_instance_list.append(instance.id)
    
        for dead_instance in self.dead_instance_list:
            reactivate_EBS(self, dead_instance.id)

    '''
    Add a new instance to the datastructure 
    @return true on success
    '''
    def add_instance_entry(self, instance_id, ebs_id=None, state=ALIVE):
        # we always assume that when an instance is ALIVE when it is added to                                      
        # the data structure.
		
        with LOCK:
            self.inst_to_ebs[instance_id] = ebs_id
            self.ip_to_inst[get_instance_private_ip(instance_id)] = instance_id
            self.alive_instance_list.add(instance_id)

    ''' 
    Remove the given instance from the datastructure
    @return the associated state and block device id
    '''
    def remove_instance_entry(self, instance_id):
        with LOCK:
            self.inst_to_ebs.pop(instance_id)
            self.alive_instance_list.remove(instance_id)
            instance_ip = [i for key, value in self.ip_to_inst.items() if value == instance_id][0]
            self.ip_to_inst.pop(instance_ip)

    '''
    Return the instance id associated for the given ip address.
    '''
    def get_instance_id(self, instance_ip):
        with LOCK:
            return self.ip_to_inst[instance_ip]

    '''
    Retrive the block device id for a given instance
    @return the block device idsudo id -nu
    '''
    def get_ebs_id(self, instance_id):
        with LOCK:
            return self.inst_to_ebs[instance_id]

    '''
    Retrive the state for a given instance
    @return state
    '''
    def is_dead(self, instance_id):
        with LOCK:
            return instance_id in self.dead_instance_list

    '''
    Return a list of instances with state=DEAD
    @return dead instances
    '''
    def get_dead_instances(self):
        with LOCK:
            return self.dead_instance_list
    
    '''
    Return all alive instances
    @return all alive instances
    '''
    def get_alive_instances(self):
        with LOCK:
            return self.alive_instance_list

    '''
    Update the entry in the datastructure
    '''
    def update_instance_entry(self, instance_id, ebs_id):
        with LOCK:
            self.inst_to_ebs[instance_id] = ebs_id

    def __repr__(self):
        return "alive instances: " + str(self.alive_instance_list) + "\n" \
                + "dead instances: " + str(self.dead_instance_list) + "\n"\
                + "ip_to_inst: " + str(self.ip_to_inst) + "\n"            \
                + "inst_to_ebs: " + str(self.inst_to_ebs) + "\n"          

'''
Connect to the AWS
@return true on success
'''
def connect_to_EC2(key=KEY, secret_key=SECRET_KEY):
    global conn
    conn = boto.connect_ec2(
        aws_access_key_id = key,
            aws_secret_access_key= secret_key)

    return 

'''
@return the instance object for the given instance id
'''
def get_instance(instance_id):
    return conn.get_only_instances(instance_ids=instance_id)[0]


'''
@return the instance object for the given instance id
'''
def get_all_instances(filt={"tag-key":"prime"}):
    return conn.get_only_instances(filters=filt)


'''
Returns the PRIVATE IP address associated with the given instance_id.
Note to self: This IP address will identify which nodes have failed in the log.

@instance_id
'''
def get_instance_private_ip(instance_id):
    instance = get_instance(instance_id)
    return instance.private_ip_address

'''
Returns the IP address associated with the given instance_id.
@instance_id
'''
def get_instance_ip(instance_id):
    instance = get_instance(instance_id)
    return instance.ip_address


'''
launch a predefined ubuntu image with Hadoop installed.
This is launch without an EBS attached.

Root store is ebs.

@return instance id on succesful launch.
'''
def launch_instance(ami=AMI):
    image = conn.get_image(ami)
    reserv = image.run(security_groups=["HadoopSecurityGroup"], instance_type="m1.small", key_name="hadoopKey")
    instance = reserv.instances[0]
    instance.add_tag("prime")

    print "launching instance ..."
    while(instance.update() != "running"):
        if instance.state != "pending":
            # Instance is not pending => instance won't start
            return None
        sleep(FREEZE)

    return instance.id


'''
Delete a given instance.
@return true on success
'''
def delete_instance(instance_id):
    instance = get_instance(instance_id)
    instance.terminate()

    print "deleting instance ..."
    while(instance.update() != "terminated"):
        if instance.state != "shutting-down":
            # Instance is not shutting down => instance won't shutdown
            return False
        sleep(FREEZE)

    return True


'''
@return the ebs object for the given ebs id
'''
def get_EBS(ebs_id):
    return conn.get_all_volumes(volume_ids=ebs_id)[0]

'''
launch a EBS drive
@return the ebs id on succesful launch
'''
def create_EBS(size, region="us-east-1d"):
    vol = conn.create_volume(size, region);
   
    while(vol.update() != "available"):
        if vol.status != "creating":
            # EBS is not being created => err?
            return None
        sleep(FREEZE)

    return vol.id


'''
launch a EBS drive
@return the ebs id on succesful launch
'''
def delete_EBS(ebs_id):
    vol = conn.get_all_volumes(volume_ids=ebs_id)[0];
    vol.delete();
    return True

'''
attach a given EBS drive to a given instance
@return true on success
'''
def attach_EBS(ebs_id, instance_id, device="/dev/sdz"):
    conn.attach_volume(ebs_id, instance_id, device);
    vol = get_EBS(ebs_id);
    while(vol.update() != "in-use"):
        if vol.status != "available":
            # EBS is not being attached => err?
            return False
        sleep(FREEZE)
        
    return True

'''
detach a given EBS drive from a given instance
@return true on success
'''
def detach_EBS(ebs_id, instance_id, device="/dev/sdz"):
    conn.detach_volume(ebs_id, instance_id, device)
    vol = get_EBS(ebs_id);
    while(vol.update() != "available"):
        if vol.status != "in-use":
            # EBS is not being dettached => err?
            return False
        sleep(FREEZE)

    return True

'''
start a new node with the ebs of the given dead node. 
prime contains the state of the current cluster.

@return true on success
'''
def reactivate_EBS(prime, dead_ip): 
    dead_instance_id = prime.get_instance_id(dead_ip)
    free_ebs_id = prime.get_ebs_id(dead_instance_id)

    print bcolors.OKGREEN+"REACTIVATE DEAD IP: "+str(dead_instance_id)+" AND EBS "+ str(free_ebs_id)+bcolors.ENDC
    new_instance_id = launch_instance()
    attach_EBS(free_ebs_id, new_instance_id)
    # ssh into the new instance and mount the ebs
    # ssh into the new instance and start the datanode process.
    subprocess.call(["sh", "/home/ubuntu/modify_node.sh", get_instance_ip(new_instance_id), SSH_KEY, "xvdz", MASTER])

    # need to update the conf files for the master
    with open("/etc/hosts", "a") as HOSTS:
        HOSTS.write("{ip} slave{ip}\n".format(ip=get_instance_private_ip(new_instance_id)))

    # update the slave names in the slaves conf file
    with open("/home/hduser/hadoop/hadoop-1.2.1/conf/slaves", "a") as SLAVES:
        SLAVES.write("slave{ip}\n".format(ip=get_instance_private_ip(new_instance_id)))


    prime.add_instance_entry(new_instance_id, free_ebs_id)
    prime.remove_instance_entry(dead_instance_id)

def run():
    print "STARTING EC2 AUTOMATION"
    connect_to_EC2()
    temp = []
    prime = instance_decorator();
    # prime.initialize() <--
    print(prime)

    # fork and start the process for processing commands.
    upid = os.fork()
    if upid > 0:
        print "CHILD PROCESS RUNNING THE USER INPUT LOOP"
        usr_input = raw_input(bcolors.HEADER + ">> " + bcolors.ENDC)

        while usr_input != "exit":
            if usr_input == "state":
                print prime
            if usr_input == "temp":
                print temp
            usr_input = raw_input(bcolors.HEADER + ">> " + bcolors.ENDC)

        # read from standard in, wait for commands ... do we need to write an api?
        exit(0)

    # fork and start the process for detecting dead nodes.
    r,w = os.pipe()
    r = os.fdopen(r, "r", 0)
    w = os.fdopen(w, "w", 0)
    dpid = os.fork()
    if dpid > 0:
        print "CHILD PROCESS RUNNING THE HEART BEAT CHECKER"
        #we need to clean up properly later i.e send a kill signal to child proccess
        r.close()
        while True:
            # write deadips to pipe
            print "calling run_once";
            dead_ips = run_once()
            print bcolors.OKBLUE + str(dead_ips) + bcolors.ENDC
            for ip in dead_ips:
                print >>w, "%s" % ip
                w.flush()
            sleep(LOOP_INTERVAL)

        w.close();
        exit(1)
    
    w.close()
    while True:
        dead_ip = r.readline()
        temp.append(dead_ip)
        if dead_ip:
            dead_ip = dead_ip.split(":")[0]
            reactivate_EBS(prime, dead_ip)

    print "SHOULD NEVER GET HERE"
    
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

    def disable(self):
        self.HEADER = ''
        self.OKBLUE = ''
        self.OKGREEN = ''
        self.WARNING = ''
        self.FAIL = ''
        self.ENDC = ''

if __name__ == "__main__":
    print bcolors.HEADER + "Hi :) "+ bcolors.ENDC
    run()
