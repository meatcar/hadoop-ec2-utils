import os
import boto
import json
import pipes
import subprocess
import signal
import threading

from time import sleep
from sys import exit
from checkHeartbeat import run_once, LOOP_INTERVAL, DEAD_NODE_LIST

# State of the vm on ec2
ALIVE = True
DEAD  = False
DEFAULT_EBS_SIZE = 2;

# needs to be global, no lock because instance decorator is fully synchronized.
PRIME = None
conn = None

# Load in the ec2 properties located in the specified conf file
with open("conf.json") as conf:
    PROPERTIES = json.loads(conf.read())

AMI = PROPERTIES["ami"]
SECRET_KEY = PROPERTIES["secret_key"]
KEY = PROPERTIES["key"]
MASTER = PROPERTIES["master-ip"]
REGION = PROPERTIES["region"]

with open("/home/hduser/.ssh/id_rsa.pub", "r") as sshkey:
    SSH_KEY = sshkey.read()
    SSH_KEY = '"' + SSH_KEY + '"'
LOCK = threading.Lock()
FREEZE=10

class bcolors:
    '''
    bcolors class is used to print colored messages to the terminal.
    '''
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'


PROMPT_PREFIX = bcolors.HEADER + ">> " + bcolors.ENDC

class Optimus_Prime:
    def __new__(cls, *args, **kwargs):
        '''
        Create singleton object
        '''
        if not cls._instance:
            cls._instance = super(Optimus_Prime, cls).__new__(
                    cls, *args, **kwargs)
        return cls._instance    

    def __init__(self):
        '''
        Data Structure for maintaining the state of the current cluster. This includes
        the instances, their states and their respective network block devices. 
        '''
        self.inst_to_ebs         = dict()
        self.ip_to_inst          = dict()
        self.alive_instance_list = list()
        self.dead_instance_list  = DEAD_NODE_LIST 

        # Reconstructing the state of the cluster.
        #
        # Algorithm:
        # Get all currently running instances with the 'prime*' tag
        #     for each of the instances get the 
        #     1. attached EBS (secondary).
        #     2. private IP
        #     3. the state - DEAD || ALIVE
        # populate the data structure with this information.
        all_instances = get_all_instances()
        for instance in all_instances:
            # Add node to ip->inst mapping if ip can be located for the node
            if get_instance_private_ip(instance.id):
                self.ip_to_inst[get_instance_private_ip(instance.id)] = instance.id
            # Add node to inst->ebs mapping if node has an ebs in the usual location
            if instance.block_device_mapping.has_key('/dev/sdz'):
                self.inst_to_ebs[instance.id] = instance.block_device_mapping['/dev/sdz'].volume_id

            # Add node to alive instance list if its running, otherwise its dead to me.
            if instance.state == 'running':
                self.alive_instance_list.append(instance.id) 
            else:
                print "added a dead node"
                # handle the shutting down state (instance id is unavalible when in limbo)
                #self.dead_instance_list.add(instance.id)
    
        for dead_instance in self.dead_instance_list:
            # Reactivate dead nodes iff it has an ebs (i.e was not an intentional shut down)
            if dead_instance in self.inst_to_ebs.keys():
                reactivate_EBS(self, dead_instance.id)

    def add_instance_entry(self, instance_id, ebs_id=None, state=ALIVE, wait_lock_do_I_hold=False):
        '''
        Add a new instance to the datastructure
        @return true on success
        '''
        # we always assume that when an instance is ALIVE when it is added to                                      
        # the data structure.
        if not wait_lock_do_I_hold:
            LOCK.acquire()
        self.inst_to_ebs[instance_id] = ebs_id
        self.ip_to_inst[get_instance_private_ip(instance_id)] = instance_id
        self.alive_instance_list.append(instance_id)
        if not wait_lock_do_I_hold:
            LOCK.release()
         

    def remove_instance_entry(self, instance_id, wait_lock_do_I_hold=False):
        '''
        Remove the given instance from the datastructure
        @return the associated state and block device id
        '''
        if not wait_lock_do_I_hold:
            LOCK.acquire()

        # remove from ebs list 
        if instance_id in self.inst_to_ebs.keys(): 
            self.inst_to_ebs.pop(instance_id)
        # remove from alive list 
        if instance_id in self.alive_instance_list:
            self.alive_instance_list.remove(instance_id)
        # remove from ip map   
        instance_ip = [key for key, value in self.ip_to_inst.items() if value == instance_id][0]
        if instance_ip in self.ip_to_inst.keys(): 
            self.ip_to_inst.pop(instance_ip)

        if not wait_lock_do_I_hold:
            LOCK.release()

    def get_instance_id(self, instance_ip, wait_lock_do_I_hold=False):
        '''
        Return the instance id associated for the given ip address.
        '''
        if wait_lock_do_I_hold:
            return self.ip_to_inst[instance_ip]
        with LOCK:
            return self.ip_to_inst[instance_ip]

    def get_ebs_id(self, instance_id, wait_lock_do_I_hold=False):
        '''
        Retrive the block device id for a given instance
        @return the block device idsudo id -nu
        '''
        if wait_lock_do_I_hold:
            return self.inst_to_ebs[instance_id]
        with LOCK:
            return self.inst_to_ebs[instance_id]

    def is_master(self, instance_id):
        '''
        Return true iff the given instance_id is the master node
        @return boolean
        '''
        return MASTER == get_instance_private_ip(instance_id)  

    def is_dead(self, instance_id):
        '''
        Retrive the state for a given instance
        @return state
        '''
        with LOCK:
            return instance_id in self.dead_instance_list

    def get_dead_instances(self):
        '''
        Return a list of instances with state=DEAD
        @return dead instances
        '''
        with LOCK:
            return self.dead_instance_list
    
    def remove_slaves(self):
        '''
        Remove all slave nodes in the current cluster
        @return number of nodes deleted
        '''
        counter = 0
        with LOCK:
            for instance in self.alive_instance_list:
                inst = get_instance(instance)
                inst.update()
                if not self.is_master(instance) and inst.state == "running":
                    print "Working with instance: %s, ip: %s" %(instance, get_instance_private_ip(instance))
                    # update slaves file, remove the instance entry
                    with open("/home/hduser/hadoop/hadoop-1.2.1/conf/slaves", "r+") as SLAVES:
                        old_slaves = SLAVES.readlines()
                        new_slaves = [line for line in old_slaves if line.strip(' \t\n\r') \
                                != str(get_instance_private_ip(instance))]
                        SLAVES.seek(0)
                        SLAVES.writelines(new_slaves)
                        SLAVES.truncate()


                    detach_EBS(self.get_ebs_id(instance, True), instance)
                    delete_EBS(self.get_ebs_id(instance, True))
                    delete_instance(instance)
                    self.remove_instance_entry(instance, True)
                    counter+=1

        return counter

    def remove_instance_and_ebs(self, instance_id):
        '''
        Remove instance from cluster and structure.
        @instance_id is the id of the instance to be removed.
        '''
        with LOCK:
            if not self.is_master(instance_id):
                # update slaves file, remove the instance entry
                with open("/home/hduser/hadoop/hadoop-1.2.1/conf/slaves", "r+") as SLAVES:
                    old_slaves = SLAVES.readlines()
                    new_slaves = [line for line in old_slaves if line.strip(' \t\n\r') \
                            != str(get_instance_private_ip(instance_id))]
                    SLAVES.seek(0)
                    SLAVES.writelines(new_slaves)
                    SLAVES.truncate()

                detach_EBS(self.get_ebs_id(instance_id, True), instance_id)
                delete_EBS(self.get_ebs_id(instance_id, True))
                delete_instance(instance_id)
                self.remove_instance_entry(instance_id, True)

    def get_alive_instances(self):
        '''
        Return all alive instances
        @return all alive instances
        '''
        with LOCK:
            return self.alive_instance_list

    def update_instance_entry(self, instance_id, ebs_id):
        '''
        Update the entry in the datastructure
        @instance_id
        @ebs_id
        '''
        with LOCK:
            self.inst_to_ebs[instance_id] = ebs_id

    def __repr__(self):
        '''
        Pretty printer for class
        '''
        return "alive instances: " + str(self.alive_instance_list) + "\n" \
                + "dead instances: " + str(self.dead_instance_list) + "\n"\
                + "ip_to_inst: " + str(self.ip_to_inst) + "\n"            \
                + "inst_to_ebs: " + str(self.inst_to_ebs)           

def connect_to_EC2(key=KEY, secret_key=SECRET_KEY):
    '''
    Connect to the AWS
    @return true on success
    '''
    global conn
    conn = boto.connect_ec2(
        aws_access_key_id = key,
            aws_secret_access_key= secret_key)

    return 

def get_instance(instance_id):
    '''
    @return the instance object for the given instance id
    '''
    return conn.get_only_instances(instance_ids=instance_id)[0]

def get_all_instances(filt={"tag-key":"prime"}):
    '''
    @return the instance object for the given instance id
    '''
    return conn.get_only_instances(filters=filt)

def get_instance_private_ip(instance_id):
    '''
    Returns the PRIVATE IP address associated with the given instance_id.
    Note to self: This IP address will identify which nodes have failed in the log.
    
    @instance_id
    '''
    instance = get_instance(instance_id)
    return instance.private_ip_address

def get_instance_ip(instance_id):
    '''
    Returns the IP address associated with the given instance_id.
    @instance_id
    '''
    instance = get_instance(instance_id)
    return instance.ip_address

def launch_instance(ami=AMI, region=REGION):
    '''
    launch a predefined ubuntu image with Hadoop installed.
    This is launch without an EBS attached.
    
    Root store is ebs.
    
    @return instance id on succesful launch.
    '''
    image = conn.get_image(ami)
    reserv = image.run(security_groups=["HadoopSecurityGroup"], instance_type="m1.small", key_name="hadoopKey", placement=region)
    instance = reserv.instances[0]
    instance.add_tag("prime")

    print "launching instance ..."
    while(instance.update() != "running"):
        if instance.state != "pending":
            # Instance is not pending => instance won't start
            return None
        sleep(FREEZE)

    return instance.id

def delete_instance(instance_id):
    '''
    Delete a given instance.
    @return true on success
    '''
    instance = get_instance(instance_id)
    instance.terminate()

    print "deleting instance ..."
    while(instance.update() != "terminated"):
        if instance.state != "shutting-down":
            # Instance is not shutting down => instance won't shutdown
            return False
        sleep(FREEZE)

    return True

def get_EBS(ebs_id):
    '''
    @return the ebs object for the given ebs id
    '''
    return conn.get_all_volumes(volume_ids=ebs_id)[0]

def create_EBS(size, region=REGION):
    '''
    launch a EBS drive
    @return the ebs id on succesful launch
    '''
    print "creating EBS ..."
    vol = conn.create_volume(size, region);
    while(vol.update() != "available"):
        if vol.status != "creating":
            # EBS is not being created => err?
            return None
        sleep(FREEZE)

    return vol.id

def delete_EBS(ebs_id):
    '''
    launch a EBS drive
    @return the ebs id on succesful launch
    '''
    print "deleting EBS ..."
    vol = conn.get_all_volumes(volume_ids=ebs_id)[0];
    vol.delete();
    return True

def attach_EBS(ebs_id, instance_id, device="/dev/sdz"):
    '''
    attach a given EBS drive to a given instance
    @return true on success
    '''
    print "attaching EBS ..."
    conn.attach_volume(ebs_id, instance_id, device);
    vol = get_EBS(ebs_id);
    while(vol.update() != "in-use"):
        if vol.status != "available":
            # EBS is not being attached => err?
            return False
        sleep(FREEZE)
        
    return True

def detach_EBS(ebs_id, instance_id, device="/dev/sdz"):
    '''
    detach a given EBS drive from a given instance
    @return true on success
    '''
    print "detaching EBS ..."
    conn.detach_volume(ebs_id, instance_id, device, force=True)
    vol = get_EBS(ebs_id);
    while(vol.update() != "available"):
        if vol.status != "in-use":
            # EBS is not being dettached => err?
            return False
        sleep(FREEZE)

    return True

def reactivate_EBS(dead_ip): 
    '''
    start a new node with the ebs of the given dead node.
    prime contains the state of the current cluster.
    
    @return true on success
    '''
    dead_instance_id = PRIME.get_instance_id(dead_ip, True)
    free_ebs_id = PRIME.get_ebs_id(dead_instance_id, True)

    print bcolors.OKGREEN + "REACTIVATE DEAD IP: " + str(dead_instance_id) \
                          +  " AND EBS " + str(free_ebs_id) + bcolors.ENDC
    new_instance_id = launch_instance()
    # get state of the ebs, check if detached if not, detach it
    attach_EBS(free_ebs_id, new_instance_id)
    #arbitary sleep amount since we cant ssh immediately ...
    sleep(FREEZE+FREEZE)
    prompt("SETTING UP NEW NODE")
    sleep(FREEZE+FREEZE)
    # ssh into the new instance and mount the ebs
    # ssh into the new instance and start the datanode process.
    subprocess.call(["sh", "/home/hduser/hadoop/relaunch_node.sh", get_instance_ip(new_instance_id), SSH_KEY, "xvdz", MASTER])

    # update the slave names in the slaves conf file
    with open("/home/hduser/hadoop/hadoop-1.2.1/conf/slaves", "a") as SLAVES:
        SLAVES.write(str(get_instance_private_ip(new_instance_id))+"\n")

    # update slaves file, remove the dead_ip entry 
    with open("/home/hduser/hadoop/hadoop-1.2.1/conf/slaves", "r+") as SLAVES:
        old_slaves = SLAVES.readlines()
        new_slaves = [line for line in old_slaves if line.strip(' \t\n\r') != str(dead_ip)]
        SLAVES.seek(0)
        SLAVES.writelines(new_slaves)
        SLAVES.truncate()

    print "Updating state ..."
    PRIME.add_instance_entry(new_instance_id, free_ebs_id, wait_lock_do_I_hold=True)
    PRIME.remove_instance_entry(dead_instance_id, wait_lock_do_I_hold=True)
    prompt("Reactivtion of new node " + str(new_instance_id) + " complete!", bcolors.OKGREEN)

# Launch a new slave with the EBS of the given size.
def launch_slave(size):
    new_instance_id = launch_instance()
    new_ebs_id = create_EBS(size)
    attach_EBS(new_ebs_id, new_instance_id)
    
    # ssh into the new instance and mount the ebs
    # ssh into the new instance and start the datanode process.
    prompt("SETTING UP NEW NODE")
    sleep(FREEZE+FREEZE)
    subprocess.call(["sh", "/home/hduser/hadoop/launch_node.sh", get_instance_ip(new_instance_id), SSH_KEY, "xvdz", MASTER])

    # update the slave names in the slaves conf file
    with open("/home/hduser/hadoop/hadoop-1.2.1/conf/slaves", "a") as SLAVES:
        SLAVES.write(str(get_instance_private_ip(new_instance_id))+"\n")

    PRIME.add_instance_entry(new_instance_id, new_ebs_id)
    prompt("Launched node: " + new_instance_id, bcolors.OKGREEN) 

def poll_reactivation(r):
    '''
    Reads dead ips from file r and does the reactivation when neccassary
    '''
    while True:
        dead_ip = r.readline()
        if dead_ip:
            dead_ip = dead_ip.split(":")[0]
            with LOCK:
                if dead_ip in PRIME.ip_to_inst.keys() \
                        and PRIME.ip_to_inst[dead_ip] in PRIME.inst_to_ebs.keys():
                    reactivate_EBS(dead_ip)

    r.close()
    exit(1)


def prompt(message="", col=bcolors.HEADER):
    '''
    Simple user prompt of format '>> Msg' 
    '''
    end = bcolors.ENDC

    if col == "":
        end = ""
    else:
        start = col

    print PROMPT_PREFIX + start + str(message) + end


def get_raw_input():
    return raw_input(PROMPT_PREFIX)

def print_help_menu():
        print "Welcome to the EC2 ALLSPARK help menu"
        print "--------------------------------------"
        print "List of available commands:"
        print "1. ips - print node ips"
        print "2. state   - list the state of the cluster to which you are currently connected."
        print "3. launch - [EBS-size] [region] [AMI] - launch a new instance."
        print "4. delete - removes instance inst from the cluster, prompt will ask for id"
        print "5. exit - disconnect from EC2."
        print "6. ids - print node ids"
        print "7. help - lists this help menu."
        print ""
        print "syslab.toronto.edu"
        print ""

def HBservice():
    connect_to_EC2()
    global PRIME
    PRIME = Optimus_Prime()

    r,w = os.pipe()
    r = os.fdopen(r, "r", 0)
    w = os.fdopen(w, "w", 0)
    dpid = os.fork()

    # child runs the heartbeat checker looping for dead nodes            
    if dpid == 0:
        print "CHILD PROCESS RUNNING THE HEART BEAT CHECKER"
        #we need to clean up properly later i.e send a kill signal to child proccess
        r.close()
        while True:
            # write deadips to pipe
            dead_ips = run_once()
            #print bcolors.OKBLUE + str(dead_ips) + bcolors.ENDC
            for ip in dead_ips:
                print >>w, "%s" % ip
                w.flush()
                sleep(LOOP_INTERVAL)
        
        w.close()
        exit(1)

    # child loops waiting to preform reactivations  
    w.close()
    poll = threading.Thread(target = poll_reactivation, args = (r,)); 
    poll.daemon = True
    poll.start()


    # parent listens and executes user commands.
    print("EC2 ALLSPARK CONSOLE");
    print("version 0.0.1");
    print("type 'help' to get started.");
    cmd = get_raw_input()

    while cmd != "exit":
        if cmd == "help":
            print_help_menu()
        elif cmd == "test":
            prompt("Successfully ran all tests!")
        elif cmd == "ips":
            prompt(str(PRIME.ip_to_inst.keys()))
        elif cmd == "ids":
            prompt(str(PRIME.alive_instance_list))
        elif cmd == "state":
            print PRIME
        elif cmd == 'launch':
            # NOTE: add way for user to pass in EBS size, region, AMI etc
            launch_slave(DEFAULT_EBS_SIZE)
        elif cmd == "delete":
            # NOTE: add way for user delete with id or ip in one line of input  
            instance_id = raw_input("type in id of instance " + PROMPT_PREFIX )
            if instance_id in PRIME.alive_instance_list:
                PRIME.remove_instance_and_ebs(instance_id)
            else:
                prompt("instance not found")
        else:
            print("command not recognized. type 'help' to list available commands");

        cmd = get_raw_input()

    print("Preparing to clean up ...")
    os.kill(dpid, signal.SIGKILL)
    print("Bye")
    exit(0)

if __name__ == "__main__":
    prompt("Starting to connect", bcolors.WARNING)
    print bcolors.OKBLUE + "Hi :) "+ bcolors.ENDC
    HBservice() 
    
