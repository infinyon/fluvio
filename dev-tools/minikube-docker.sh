#!/bin/bash
nohup ssh  -o "StrictHostKeyChecking=no" -i $(minikube ssh-key) docker@$(minikube ip) -R 5000:localhost:5000 -nNT > /tmp/min.out 2> /tmp/min.out &
