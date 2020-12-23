#!/usr/bin/env bash
set -e

# Params:
# $1) the image tag to use, e.g. "fluvio:GIT_HASH"
# $2) the build directory, "debug" or "release"

if [ -n "$MINIKUBE_DOCKER_ENV" ]; then
  eval $(minikube -p minikube docker-env)
fi

tmp_dir=$(mktemp -d -t fluvio-docker-image-XXXXXX)
cp target/x86_64-unknown-linux-musl/$2/fluvio_runner_local_cli $tmp_dir/fluvio
cp $(dirname $0)/fluvio.Dockerfile $tmp_dir/Dockerfile
cd $tmp_dir
docker build -t $1 .
rm -rf $tmp_dir

if [ -n "$MINIKUBE_DOCKER_ENV" ]; then
  docker image tag $1 infinyon/$1
fi
