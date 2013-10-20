#!/usr/bin/python
'''
Check current heartbeat and find machines that need to repaired
Output: add a line to OUTPUT_FILE for each node found
'''
import subprocess
import datetime
import time
import os

TIMEOUT = 6        # in seconds, heartbeat longer than TIMEOUT, need repair
LOOP_INTERVAL = 6  # in seconds, sleep time of run_once()
OUTPUT_FILE = "nodes_to_repair.txt"

# List to keep track 
DEAD_NODE_LIST = set()

def get_report():

    return subprocess.check_output(["hadoop", "dfsadmin", "-report"])

def parse_report(report):
    '''returns a dictionary: name (str) -> heartbeats in seconds (float)'''
    
    result = dict()
    last_node = None
    lines = report.split('\n')
    for line in lines:
        entries = line.split(':', 1)
        if len(entries) == 2:
            if entries[0] == "Name":
                last_node = entries[1].strip()
            if entries[0] == "Last contact":
                time_str = entries[1].strip()
                dt = datetime.datetime.strptime(time_str, "%a %b %d %H:%M:%S %Z %Y")
                now_dt = datetime.datetime.now()
                time_diff = (now_dt - dt).total_seconds()
                if last_node is None:
                    raise RuntimeError("last_node is None, impossible")
                result[last_node] = time_diff
    return result

def add_to_output(key):

    existing_keys = set()
    if os.path.exists(OUTPUT_FILE):
        f = open(OUTPUT_FILE, 'r')
        for line in f:
            existing_keys.add(line.strip())
        f.close()
    if key not in existing_keys:
        f = open(OUTPUT_FILE, 'a')
        f.write("%s\n" % key)
        f.close()
    return

def run_once():

    report = get_report()
    info = parse_report(report)

    dead_ips = list()
    
    for key in info:
        if info[key] > TIMEOUT:
            add_to_output(key)
            DEAD_NODE_LIST.add(key)
            dead_ips.append(key)
#            PRINT "Dead datanode (%s) detected at %s" \
#                    % (key, str(datetime.datetime.now()))

    return dead_ips

def run():

#    print
#    print "Repair checker started"
#    print "Timeout is %s seconds, loop interval is %s seconds." \
#            % (str(TIMEOUT), str(LOOP_INTERVAL))
#    print "Output file is \"%s\"" % OUTPUT_FILE
#    print

    while True:
        # pass out put from this method into our open pipe
        # write_to_pipe(run_once())
        return run_once()
        time.sleep(LOOP_INTERVAL)
       
if __name__ == "__main__":
    run()
