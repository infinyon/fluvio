#!/bin/bash
kubectl config set-cluster mycube --server=https://minikubeCA:8443 --certificate-authority=.minikube/ca.crt
kubectl config set-context mycube --user=minikube --cluster=mycube
kubectl config use-context mycube