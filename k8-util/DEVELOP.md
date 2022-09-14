## Trace K8 API 

Add proxy cluster configuration to `~/.kube/config`

```yml
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

```bash
kubectl config use-context proxy
```

Set up Local Proxy to minikube.  It's easy to confingure for other K8 cluster
as well:

```bash
kubectl proxy
Starting to serve on 127.0.0.1:8001
```

Use `tcpdump` to inspect:

```bash
sudo tcpdump -i any  port 8001 -A
```
