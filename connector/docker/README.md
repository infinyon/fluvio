# Introduction

This is sample project to demonstrate how to use the docker to run the connector.
This examples is for http connector but can be used for any connector.

# To build image

To build docker image run:
```
make build
```

This will copy following files from your local file system:
- your current fluvio profile
- connector.yaml

The connector.yaml is connector config for http source


# To run image

To run image, run:
```
make run
```

This setup will run `host` network mode. This means that connector will be able to access the local network.  If you want to run in different network mode, you can modify the `Makefile` to change the network mode.