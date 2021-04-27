#!/usr/bin/env bash
set -e

readonly PROGDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Package fluvio-run into a Docker image
#
# PARAMS:
# $1: The tag to build this Docker image with
#       Ex: 0.7.4-abcdef (where abcdef is a git commit)
# $2: The path to the fluvio-run executable
#       Ex: target/x86_64-unknown-linux-musl/$CARGO_PROFILE/fluvio-run
# $3: Whether to build this Docker image in the Minikube context
#       Ex: true, yes, or anything else that is non-empty
main() {
  local -r DOCKER_TAG=$1; shift
  local -r FLUVIO_RUN=$1; shift
  local -r MINIKUBE=$1
  local -r tmp_dir=$(mktemp -d -t fluvio-docker-image-XXXXXX)

  if [ -n "$MINIKUBE" ]; then
    echo "Setting Minikube build context"
    eval $(minikube -p minikube docker-env)
  fi

  cp "${FLUVIO_RUN}" "${tmp_dir}/fluvio-run"
  chmod +x "${tmp_dir}/fluvio-run"
  cp "${PROGDIR}/fluvio.Dockerfile" "${tmp_dir}/Dockerfile"

  pushd "${tmp_dir}"
  docker build -t "infinyon/fluvio:${DOCKER_TAG}" .
  popd || true
  rm -rf "${tmp_dir}"
}

main "$@"
