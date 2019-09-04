#!/bin/bash
# initialize CRD
# add CRD 
DATA_DIR=$(dirname "$0")
kubectl apply -f ${DATA_DIR}/crd_spu.yaml
kubectl apply -f ${DATA_DIR}/crd_spg.yaml
kubectl apply -f ${DATA_DIR}/crd_partition.yaml
kubectl apply -f ${DATA_DIR}/crd_topic.yaml