# Release process

## install tools

Install necessary third party tools
```
make install_tools
```

## Update version

Bump up ```VERSION``` in the root.  This follows semver

## Run smoke test

Assumes following conditions are met:
* Minikube have setup
* Fluvio sys chart has been installed
* Tunnel and docker registry are running

Build development image

```
make minikube_image
```

Perform cargo build
```
Cargo build
```

Run integration test with 2 SPU and replicas:
```
flvt -k -r 2
```

## Upload release image to docker hub

```
make release_image
```

## Publish new helm chart

```
make helm_package
```

This will create new helm chart tar file and copy to chart repo.

cd to chart repo, commit and push the changes to master

## Release CLI

```
make create_release
make upload_release
```
