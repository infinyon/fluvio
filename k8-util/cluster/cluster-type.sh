#!/bin/bash
# return type of cluster:  minikube,k3d,aws
set -e
nodes=`kubectl get nodes -o=jsonpath='{.items[0].metadata.name}'`


if echo ${nodes} | grep -q minikube;  then
    echo 'minikube'
elif echo ${nodes} | grep -q k3d; then
    echo "k3d"
else 
    "unknown"
fi
