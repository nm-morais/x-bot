
if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify env var DOCKER_IMAGE"
  exit
fi

docker ps -a | awk '{ print $1,$2 }' | grep $DOCKER_IMAGE | awk '{print $1 }' | xargs -I {} docker rm -f {}
