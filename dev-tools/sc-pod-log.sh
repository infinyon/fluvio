#!/bin/bash

kubectl logs  "$@" `(kubectl get pod -l app=fluvio-sc  -o jsonpath="{.items[0].metadata.name}")`