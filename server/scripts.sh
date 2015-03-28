#!/bin/sh

#1. Открыть порты хадупа наружу
#2. Залить файл в хдфс
#3. Посчитать что-нибудь

#start hadoop and login to console
docker run -P -i -t sequenceiq/hadoop-docker:2.6.0 /etc/bootstrap.sh -bash

#start hadoop as a daemon and open ports #FIXME close after full init :(
docker run -d -P sequenceiq/hadoop-docker:2.6.0 /etc/bootstrap.sh
#ps running containers
docker ps -l

#info by name
docker inspect admiring_engelbart | less

