#!/bin/sh

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify env var DOCKER_IMAGE"
  exit
fi

docker build --build-arg LATENCY_MAP=$LATENCY_MAP --build-arg IPS_FILE=$IPS_FILE --file build/test.Dockerfile . -t $DOCKER_IMAGE