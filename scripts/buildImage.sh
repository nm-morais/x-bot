#!/bin/sh

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify env var DOCKER_IMAGE"
  exit
fi

(cd ../go-babel && ./scripts/buildImage.sh)

docker build --build-arg LATENCY_MAP=$LATENCY_MAP --build-arg IPS_FILE=$IPS_FILE --file build/Dockerfile . -t $DOCKER_IMAGE