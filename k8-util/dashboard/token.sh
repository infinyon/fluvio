#!/bin/bash
kubectl -n kubernetes-dashboard describe secret $(kubectl -n kubernetes-dashboard get sa/admin-user -o jsonpath="{.secrets[0].name}") 