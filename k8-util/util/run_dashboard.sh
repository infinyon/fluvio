#!/bin/bash
# 
kubectl -n kube-system  port-forward $(kubectl -n kube-system  get pod -l k8s-app=kubernetes-dashboard -o jsonpath='{.items[0].metadata.name}') 8443:8443  &
