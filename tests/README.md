# Local test
This run test with SC and SPU running in your dev machine.

## Setting up

```
cargo build
alias flvd=./target/debug/fluvio

```


# Running simple test

This run a simple smoke test with a single record

```
$ flvt --local-driver --log-dir /tmp
```

Displaying current offsets:
```
$ flvd partition list
 TOPIC   PARTITION  LEADER  REPLICAS  RESOLUTION  HW  LEO  LSR  FOLLOWER OFFSETS 
 topic0  0          5001    []        Online      1   1    0    [] 
```

Run a test with sending 10 records:
```
flvt  --produce-iteration 10  -d

no setup
no topic initialized
start testing...
found topic: topic0 offset: 1
starting fetch stream for: topic0 at: 1, expected new records: 10
consumer: received batches 0
produced message topic: topic0, offset: 1,len: 108
consumer: received batches 1
   consumer: total records: 1, validated offset: 2
produced message topic: topic0, offset: 2,len: 108
consumer: received batches 1
   consumer: total records: 2, validated offset: 3
produced message topic: topic0, offset: 3,len: 108
consumer: received batches 1
   consumer: total records: 3, validated offset: 4
produced message topic: topic0, offset: 4,len: 108
consumer: received batches 1
   consumer: total records: 4, validated offset: 5
produced message topic: topic0, offset: 5,len: 108
consumer: received batches 1
   consumer: total records: 5, validated offset: 6
produced message topic: topic0, offset: 6,len: 108
consumer: received batches 1
   consumer: total records: 6, validated offset: 7
produced message topic: topic0, offset: 7,len: 108
consumer: received batches 1
   consumer: total records: 7, validated offset: 8
produced message topic: topic0, offset: 8,len: 108
consumer: received batches 1
   consumer: total records: 8, validated offset: 9
produced message topic: topic0, offset: 9,len: 108
consumer: received batches 1
   consumer: total records: 9, validated offset: 10
produced message topic: topic0, offset: 10,len: 109
consumer: received batches 1
   consumer: total records: 10, validated offset: 11
<<consume test done for: topic0 >>>>
consume message validated!
```