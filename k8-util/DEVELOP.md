## Trace K8 API 

Set up Local Proxy to minikube.  It's easy to confingure for other K8 cluster as well:

```
kubectl proxy
Starting to serve on 127.0.0.1:8001
```
Add proxy cluster configuration to ~/.kube/config
```
clusters:
- cluster:
    server: http://localhost:8001
  name: proxy

contexts:
- context:
    cluster: proxy
    user: minikube
  name: proxy
```

Change kubectl to use proxy:
```
kubectl config use-context proxy
```

Use tcpdump to inspect:
```
 sudo tcpdump -i any  port 8001 -A
```



