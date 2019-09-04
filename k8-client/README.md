# Kubernetes Rust Client

This crate is used to get, list, update and delete Kubernetes Objects.
This is similar to Kubernetes Go Client:  https://github.com/kubernetes/client-go

Make sure you have setup your access to kubernetes cluster

## create simple nginx pod
```kubectl create -f https://k8s.io/examples/pods/simple-pod.yaml```

## Get Pods using curl
```curl --header "Authorization: Bearer $TOKEN" --insecure $APISERVER/api/v1/namespaces/default/pods```

## Get Topics
``` curl --header "Authorization: Bearer $TOKEN" --insecure $APISERVER/apis/fluvio.infinyon.com/v1/namespaces/default/topics```


## Update topic status

```curl -X PUT -H "Content-Type: application/json" -d '{"apiVersion":"fluvio.infinyon.com/v1","kind":"Topic","metadata":{"name":"test","namespace":"default","resourceVersion":"3958"},"status":{"partitions":3}}' $APISERVER/apis/fluvio.infinyon.com/v1/namespaces/default/topics/test/status --header "Authorization: Bearer $TOKEN" --insecure```


## Running K8 integration test

By default, K8 dependent features are turn off.  In order to K8 related integrations test.  You must apply crd objects in the ```kub-rs``` crates.

Then run following commands.  This must be done in the k8-client crate.  Rust does not support workspace level feature yet.
```cargo test --features k8```
