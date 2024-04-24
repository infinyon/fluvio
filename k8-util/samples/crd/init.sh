#!/bin/bash
# initialize CRD
# Please add CRD 
DATA_DIR=$(dirname "$0")
kubectl apply -f ${DATA_DIR}/crd_spu.yaml
kubectl apply -f ${DATA_DIR}/crd_cluster.yaml
kubectl apply -f ${DATA_DIR}/crd_partition.yaml
kubectl apply -f ${DATA_DIR}/crd_topic.yaml
kubectl apply -f ${DATA_DIR}/crd_mirror.yaml
