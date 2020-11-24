#!/usr/bin/env bash
set -e

if [ -n "$MINIKUBE_DOCKER_ENV" ]; then
  eval $(minikube -p minikube docker-env)
fi

tmp_dir=$(mktemp -d -t fluvio-docker-image-XXXXXX)
cp target/x86_64-unknown-linux-musl/$CARGO_PROFILE/fluvio_runner_local_cli $tmp_dir/fluvio
cp $(dirname $0)/fluvio.Dockerfile $tmp_dir/Dockerfile
cd $tmp_dir
docker build -t infinyon/fluvio:$DOCKER_TAG .
rm -rf $tmp_dir
