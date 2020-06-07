# Test Scenarios


## Sequential Scenario

Write and Read Sequential:

Iteration: 5000,
Record size: 5k
Log size: 25M

### 1 SPU
No Issue

### 2 SPU

```
flvt --local-driver -p 5000 --record-size 5000 --spu 2 --replication 2
```




