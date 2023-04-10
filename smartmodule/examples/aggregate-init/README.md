In order to build and use this SmartModule in your cluster, follow these steps:

1. Install `smdk` tool:
```bash
fluvio install smdk
```
2. Build:
```bash
smdk build
```

3. Load built SmartModule into the cluster:
```bash
smdk load
```

After that, you can consume from your topic and apply the aggregation as trasnformation:
```bash
fluvio consume test-aggr --transforms-file transforms.yaml
```
