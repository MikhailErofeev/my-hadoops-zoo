#!/bin/sh

#add route to internal gateway or something like that i don't care to our docker virtual machine ip  ("/etc/bash boot2docker ip" to known it)
sudo route add -net 172.17.0.0/16 192.168.59.103

#start hadoop as a daemon, open ports some ports, name it had00p
#map ports to virtual machine address
#1 -d -- run docer as a daemon, 2 -d -- run bootstrap.sh in inf loop
#https://github.com/sequenceiq/docker-hadoop-ubuntu/blob/master/bootstrap.sh
#50070 - http namenode
#9000 - hdfs namenode
#8032 - yarn resourcemanager http
#8088 - yarn resourcemanager
docker run -d \
  -p 50070:50070 \
   -p 9000:9000 \
   -p 8032:8032 \
   -p 8088:8088 \
   --name had00p sequenceiq/hadoop-docker:2.6.0 /etc/bootstrap.sh -d
#look for init process of bootstrap
docker attach --sig-proxy=false had00p

#namenode web-interface http://192.168.59.103:50070/

docker exec -it had00p bash

#ps containers (-l -- last, -a -- all)
docker ps
docker inspect had00p | less
docker kill JOB_ID
docker kill had00p && docker rm had00p


# ports
# http://blog.cloudera.com/blog/2009/08/hadoop-default-ports-quick-reference/
#HDFS
# Namenode (http)	50070	dfs.http.address
# Namenode (hdfs)	9000
# Datanodes	50075	dfs.datanode.http.address
# Secondarynamenode	50090	dfs.secondary.http.address
# Backup/Checkpoint node?	50105	dfs.backup.http.address
#MR
# Jobracker	50030	mapred.job.tracker.http.address
# Tasktrackers	50060	mapred.task.tracker.http.address


