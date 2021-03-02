#!/bin/bash

set -e

if [ -z $SWARM_NET ]; then
  echo "Pls specify env var SWARM_NET"
  exit
fi

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify env var DOCKER_IMAGE"
  exit
fi

if [ -z $IPS_FILE ]; then
  echo "Pls specify env var IPS_FILE"
  exit
fi

if [ -z $LATENCY_MAP ]; then
  echo "Pls specify env var LATENCY_MAP"
  exit
fi

if [ -z $SWARM_VOL ]; then
  echo "Pls specify env var SWARM_VOL"
  exit
fi


echo "SWARM_NET: $SWARM_NET"
echo "DOCKER_IMAGE: $DOCKER_IMAGE"
echo "IPS_FILE: $IPS_FILE"

currdir=$(pwd)

docker ps -a | awk '{ print $1,$2 }' | grep $DOCKER_IMAGE | awk '{print $1 }' | xargs -I {} docker rm -f {}
docker swarm init || true
docker network create -d overlay --attachable --subnet $SWARM_SUBNET $SWARM_NET || true
mkdir $SWARM_VOL || true

bootstrap_peer_full_line=$(head -n 1 $IPS_FILE)
bootstrap_peer=$(echo "$bootstrap_peer_full_line" | cut -d' ' -f 1)

echo "bootstrap peer: $bootstrap_peer"
echo "Building image..."
bash scripts/buildImage.sh

nContainers=$(wc -l $IPS_FILE)
echo "Lauching containers..."
i=0

while read -r ip name
do
  echo "Starting container with ip $ip and name: $name"
  docker run -e config='/config/exampleConfig.yml' --cap-add=NET_ADMIN --net $SWARM_NET -v $SWARM_VOL:/tmp/logs -d -t --name "node$i" --ip $ip $DOCKER_IMAGE $i $nContainers -bootstraps="$bootstrap_peer" -listenIP="$ip"
  i=$((i+1))
done < "$IPS_FILE"


