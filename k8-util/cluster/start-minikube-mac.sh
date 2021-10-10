#!/bin/bash
set -x
sudo mv /var/db/dhcpd_leases /var/db/dhcpd_leases.old
sudo touch /var/db/dhcpd_leases
brew install hyperkit
brew install minikube
minikube config set memory 16384
minikube start --driver hyperkit --kubernetes-version=1.21.2
kubectl get nodes