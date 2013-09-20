# TODO:
# - Does AWS automagically remove dead instances?

#------------------------------------------------------------------------------#
# Script for launch amazon EC2 instances and moving EBS devices around.        #
# author: Abdi Dahir, Karan Dhiman                                             #
# date: August 12th, 2013                                                      #
#                                                                              #
# ami-7d1d977c                                                                 #
#------------------------------------------------------------------------------#
import boto
from time import sleep
import json

# State of the vm on ec2
ALIVE = True
DEAD = False

# Load in the ec2 properties located in the specified conf file
PROPERTIES = json.loads(open("conf.json").read())
AMI = PROPERTIES["ami"]
SECRET_KEY = PROPERTIES["secret_key"]
KEY = PROPERTIES["key"]
conn = None
#REGION = PROPERTIES["region"]

FREEZE=10
class instance_decorator:
    '''
    Datastructure for maintaining instances, their states and their respective 
    network block devices
    '''
    def __init__(self):
        # { instance_id: (ebs_id, state) }
        self.instance_map = {}    
        # INIT BASED OFF OF THE CONNECTION 
        # LOAD ALL EXISTING INSTANCES AND VOLUMES INTO DATASTRUCTURE		
    
    '''
    Add a new instance to the datastructure 
    @return true on success
    '''
    def add_instance_entry(self, instance_id, ebs_id=None, state=ALIVE):
        return
    
    ''' 
    Remove the given instance from the datastructure
    @return the associated state and block device id
    '''
    def remove_instance_entry(self, instance_id):
        return 

    '''
    Retrive the block device id for a given instance
    @return the block device id
    '''
    def get_ebs_id(self, instance_id):
        return
    '''
    Retrive the state for a given instance
    @return state
    '''
    def get_state(self, instance_id):
        return
    '''
    Return a list of instances with state=DEAD
    @return dead instances
    '''
    def get_dead_instances(self):
        return 
    
    '''
    Return all instances
    @return all instances
    '''
    def get_all_instances(self):
        return

    '''
    Update the entry in the datastructure
    @return true on success
    '''
    def update_instance_entry(self, instance_id):
        return
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
    return conn.get_all_instances(instance_ids=instance_id)[0].instances[0]

'''
launch a predefined ubuntu image with Hadoop installed.
This is launch without an EBS attached.

Root store is local.

@return instance id on succesful launch.
'''
def launch_instance(ami=AMI):
    image = conn.get_image(ami)
    reserv = image.run()
    instance = reserv.instances[0]

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
if __name__ == "__main__":
    print "hi"
    #connect_to_EC2()
    #instance_id = launch_instance()
    #print instance_id
    #instance = get_instance(instance_id)
    #instance.update()
    #print instance.status
    #sleep(20)
    #delete_instance(instance_id)
    #instance.update()

