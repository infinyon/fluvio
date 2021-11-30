# Deploy Components


Deploy customized metric server to disable TLS certificate check for kind.

```
kubectl apply -f metric-server.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.4.0/aio/deploy/recommended.yaml
kubectl apply -f user.yaml 
```

# Start proxy

Get token from running this script

```
$ ./token.sh
```

Then run proxy:
```
kubectl proxy
```

# Open dashboard on browser

Open browser and go to:
http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/

