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

## Election Scenario

Create cluster

```
fluvio cluster start --spu 3 --local
``

Create topic with replica 3
```
fluvio topic create -r 3 topic
```

Produce message

Identity a leader:
```
fluvio partition list
```

Read message
```
fluvio consume topic -B
```

Kill a leader SPU
```
ps -ef | grep spu
```

2nd SPU should take over, this should still work:
```
flvd consume topic -B
```




