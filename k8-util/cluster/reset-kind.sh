#!/bin/bash
# delete and re-install minikube ready for fluvio
# this defaults to docker and assume you have have sudo access
set -e
kind delete cluster
kind create cluster