#!/bin/bash
pkill tunnel
nohup  minikube tunnel  > /tmp/tunnel.out 2> /tmp/tunnel.out &
