# Overview

By default, Fluvio's leverage compiled-in charts for Kubernetes deployment.   At build time, charts are compiled as an inline resource into Cluster installer.  There is no need to locate and load charts from outside.

## Sys Chart

This chart contains CRD definitions. Since CRD is scope to a cluster, only one sys chart can be deployed to a Kubernetes cluster.

## App Chart

This chart contains definitions for Kubernetes objects such as Deployments, Services, Pods, etc.   Since it is name scoped, it can be deployed to multiple namespaces for multi-tenant configuration.

## Versioning

Each chart contains its chart version.  The chart version should be changed if and only if chart contents are changed.
The packaged chart uses the app version, the fluvio cluster version.


# Using Fluvio cluster installer

Fluvio cluster installer are build into CLI.  At the top of the repo:
```
$ make -C k8-util/helm clean
$ make build-cli
```

Then to install sys chart:
```
$ fluvio cluster start --sys-only
installing sys chart
```

List helm charts:
```
$ helm list
```

To uninstall sys chart:
```
fluvio cluster delete --sys
```

To uninstall sys chart:
```
$ helm uninstall fluvio-sys
```

# Updating Helm Charts Only

at top of repo:
```
make -C k8-util/helm clean; make helm_pkg; make build-cli;helm delete fluvio-sys;flvd cluster start --sys-only
```

