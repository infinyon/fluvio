#!/bin/bash
set -x
brew install minikube
brew install hyperkit
minikube config set memory 1024
ls -la $(which docker-machine-driver-hyperkit)
mkdir -p /var/db
touch /var/db/dhcpd.leases
minikube start --driver=hyperkit
kubectl get nodes