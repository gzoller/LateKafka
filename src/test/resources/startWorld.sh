#!/bin/bash

extra=""
if [ ! -z "$1" ]; then
  extra="-v $1:/extra"
fi

ACTIVE=`docker-machine active 2>/dev/null`
HOST_IP=`docker-machine ip $ACTIVE`

# TODO: If HOST_IP not passed in, assume AWS and hit AWS's host-getter URL
#
docker run -d -P -v ~/.docker/machine/certs:/mnt/certs -e "DOCKER_TLS_VERIFY=true" -e HOST_IP=$HOST_IP $extra gzoller/world
