#!/bin/bash
# Used for lookup up dns entry
# kubectl run -i --tty --rm debug --image=busybox --restart=Never -- sh
kubectl exec -ti busybox -- nslookup $1
