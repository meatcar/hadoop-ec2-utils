#!/bin/bash
USER="hduser"
PASS="hduser"
GROUP="hadoop"
PEM="/home/hduser/hadoopKey.pem"
HADOOP_LOCATION="ubuntu@ec2-23-20-76-61.compute-1.amazonaws.com"


echo "BOOTSTRAPPING HADOOP: PHASE JAVA";
apt-get -y update;
echo "UPDATED HADOOP: INSTALLING JAVA";
apt-get install -y openjdk-7-jre;
apt-get install -y openjdk-7-jdk;
echo "JAVA INSTALLED: SETTING UP NEW USER";
addgroup $GROUP;
#useradd -g $GROUP -p $PASS $USER;
adduser --ingroup hadoop hduser --quiet;
#echo "hduser" | passwd hduser --stdin;
echo "USER ADDED: SETTING UP THE ENVIORNMENT FOR THE USER";
echo "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64" >> /home/$USER/.bashrc
COMMAND=su -l $USER -c 'echo "GENERATING KEYPAIR FOR USER";ssh-keygen -t rsa -P "" -f /home/$USER/.ssh/id_rsa;cat .ssh/id_rsa.pub >> .ssh/authorized_keys;exit;'

# You would not believe the effort it takes to create a multiline string 
# that contains \n and - while being formatted identically to a pub key file.
echo "CREATING THE HADOOP KEY"; 
echo -n ----- > $PEM
printf "BEGIN RSA PRIVATE KEY" >> $PEM
echo -n ----- >> $PEM
printf "\n" >> $PEM
printf "MIIEpAIBAAKCAQEAt3S9EiU3Zb0ZBcypANiwe/SMZN94Fgg1AU0BnPyAmOnaubLvVHxWptSanfzi\nqZvPC892vz7o1BPA11uBBlD/ane8eYeU9qXe3DQdkfzbFbHDvopExnjVLFr7CGYf4KMtZh6MO70B\nylds3z7j7/AWXcf82iGoFncrAU5hUs10KefN242YYBgq+0TLDFNMwuGByCysXHVNzYPfmeiuxsVa\nXpWwx5XMFfrcYOjpEpo9+XdGeANUosEq2yG3TyOIxqGcm6/W/6wPdTi1s5BAPOrSb++oeRQgARCT\nrYDwt/cbJtJoxyfm7EaqJKzz3ViR26De1lIFSrAf1Bf3FTxuxqxWvQIDAQABAoIBAFLHtljtuiWK\nQjQ/uTLMlDYajw8lVwZ6BZDksomn1k8plqycdSnmQSNeY7ZOWei2iaKGpsp9FIo9r5J9k6bJ/baI\n+N7NswXoqrsqOP2a3zDW2cCsm4QDGg29r/CGRHQUrXOa7SQ3kQtAj7vcoPWvnCgNCadT8IjSlPEt\n8PRwCK6vSrpn6/7zw7HzPThk34e77mpKLnh/HsummKr++yWOOU85WdGgh/Rl0RyT+dqW2XMHjlUF\noreLfk91OoUOIEXbsQ+uUEdiJJephDfGKpBqJe8W+IJ+GwR8F982gNI6SC7B/tkKvzfQMmNoDTzG\n7N0siqsp3tX7MtdIvbSy4n3uufECgYEA2uI1BAljAZn/QUE152An1/YN2ukBHLAOWSf/s/xPLiAQ\n37AZVIa1VOcd/NsDzYCf2x7I6sY492MSn5pC6oIiZrE3TCOKtPAvss67J6GRjmoNHhYUu0I8MgOI\nXbCKmWpp6dYTdaG9xfRi5UU4hVyUfsCe5YrQEyXVcB4qBxamCuMCgYEA1pCcJrffw9Z1tkrw3a0B\nbWB334VpZO2gbPrmHyUaqZ4IW2b4YnarvG9TZxZfulDfcOsl6piBOZKNiNlVnNA39PXeYb4WYH6c\nYueDQF8Oamn8GVmx4muX2YzCc5S+7wLJZ2dIqIjD4Jc/+ZF5r1AvdEZqbIUGlSkEd2esx+ATqd8C\ngYEAweKGG6FE9fI3TNI6sU1XfdESzMqAlazNyOPJeOESMkVtLPeMOHdqwF5JDeXjJHG/KBXI203i\nwCAeKwo2JAxJ9LUdym/hOg1X8wd0eNKBYKlIHVJp4sX1FEhUzsjGvMaqMtvSOOygcWzc/UGno1oy\nn2R4W9PuJEfrHHlbuROy2QkCgYEAryIoIr5RCfOh7yJqSjZthM6J6ctmhHglSu7cKK3Lfok8fwak\nVRJSCSPBKtKrXlOmne+B6mOCjvCdUAc9hxq5oduSIavbbfXKRjx7+G3eQWOy3ypENPDnaC4phdfy\npPpcVGeMeevwHgC2uklQzpUftsPHGD7YaaWqPKBF+nte8XcCgYAhwaxjHh15Tw9BpjoeEaUTtyvE\nCkn5jz/RCf7SugG95pqrjFeqHywOZ7/4luQZd/ZQ0P7PhPS3W3zVqXNYRRUUBz9c+YohJkOCZVsX\nAQYYMD0cBOSgqKZ9DIqsSEPVicICE1c48MPppuUZDzN+ijiYQTwMeasg15w/bawmhZiL6w==\n" >> $PEM
echo -n ----- >> $PEM
printf "END RSA PRIVATE KEY" >> $PEM
echo -n -----  >> $PEM 
chmod 700 $PEM; 
chown $USER:$GROUP $PEM

echo "COPYING OVER HADOOP";
scp -i $PEM $HADOOP_LOCATION:/home/hduser/hadoop.tar.gz /home/hduser/;
chown $USER:$GROUP /home/hduser/hadoop.tar.gz;

su -l $USER -c 'tar -xvf hadoop.tar.gz;'

