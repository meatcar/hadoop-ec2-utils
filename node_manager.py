import pysftp
import sys
import os

class NodeManager(object):
    key = None

    def __init__(self, key, node_name):
        self.key = key
        self.node_name = node_name

        print "connecting to " + node_name
        self.ssh = pysftp.Connection(
                host=self.node_name,
                username="ubuntu",
                private_key=self.key + '.pem'
                )

        self.ip = self.run("dig +short %s" % self.node_name)[0].strip()

    def cleanup(self):
        self.ssh.close()

    def run(self, cmd):
        print cmd
        return self.ssh.execute(cmd)

    def copy_key(self):
        print "Copying key"
        keyname = self.key+".pem"
        self.ssh.put(keyname)
        self.run("chmod 0600 %s" % keyname)

    def setup(self):
        self.run("echo '0.0.0.0 master' | sudo tee -a /etc/hosts")
        print "Intalling packages"
        self.run("sudo apt-get update")
        # can't disable prompts..
        #self.run("DEBIAN_FRONTEND=noninteractive sudo apt-get -y upgrade")
        self.run("sudo apt-get -y install default-jre")
        self.run("echo 'JAVA_HOME=/usr/lib/jvm/default-java' | sudo tee -a /etc/environment")

    def setup_hadoop(self):
        print "Moving config"
        for f_name in os.listdir("etc/hadoop"):
            f_name = "etc/hadoop/%s" % f_name
            print "copying %s to hadoop/%s" % (f_name, f_name)
            self.ssh.put(f_name, "hadoop/%s" % f_name)

    def get_hadoop(self):
        print "Fetching latest Hadoop"
        self.run("curl http://mirror.csclub.uwaterloo.ca/apache/hadoop/common/hadoop-2.4.0/hadoop-2.4.0.tar.gz | tar -xz")
        self.run("mv hadoop-2.4.0 hadoop")

    def add_cluster_config(self, slave_names, master_name, master_ip):
        # make sure to use instance's internal ip if master
        # otherwise master can't bind to ports
        if master_name == self.node_name:
            master_ip = self.ip

        self.run('sudo sed -i "s/[0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\+ master/%s master/" /etc/hosts' % master_ip)

        slaves = '\n'.join(slave_names)
        slaves = map(
                lambda x -> self.run("dig +short %s" % x)[0].strip(),
                slaves)
        self.run("echo -n '%s\n' > hadoop/etc/hadoop/slaves" % slaves)

        # add each host to known_hosts for ssh purposes
        sshconfig_template = '\n'.join(["Host %%s",
                                    "    Hostname %%s",
                                    "    User ubuntu",
                                    "    IdentityFile ~/%s.pem"]) % self.key
        self.run("touch ~/.ssh/known_hosts")
        for name in slave_names + [master_name]:
            if name == self.node_name:
                continue #skip self

            ip = self.run("dig +short %s" % name)[0].strip()
            self.run("ssh-keygen -R %s" % ip)
            self.run("ssh-keygen -R %s" % name)

            self.run("echo > ~/.ssh/known_hosts")
            self.run("ssh-keyscan -H %s >> ~/.ssh/known_hosts" % ip)
            self.run("ssh-keyscan -H %s >> ~/.ssh/known_hosts" % name)
            self.run("echo -n '%s\n' >> ~/.ssh/config" % (
                sshconfig_template % (name, name)
                ))

    def mount_volume(self):
        print "mounting volume"
        self.run("sudo mkdir /mnt/hadoop")
        self.run("sudo mkfs.ext4 -j /dev/xvdf")
        self.run("sudo mount /dev/xvdf /mnt/hadoop")
        self.run("sudo chown -R ubuntu:ubuntu /mnt/hadoop")

    def start_hadoop(self):
        self.run("cd hadoop; bin/hdfs namenode -format")
        self.run("cd hadoop; sbin/hadoop-daemon.sh start namenode")
        self.run("cd hadoop; sbin/hadoop-daemons.sh start datanode")
        self.run("cd hadoop; sbin/yarn-daemon.sh start resourcemanager")
        self.run("cd hadoop; sbin/yarn-daemons.sh start nodemanager")
        self.run("cd hadoop; sbin/mr-jobhistory-daemon.sh start historyserver")


