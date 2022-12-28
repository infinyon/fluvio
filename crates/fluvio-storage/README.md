# Building storage cli

From root of the project
```
make build-storage-cli 
```

and set  alias for storage-cli
```
alias scli=$(PWD)/target/release/storage-cli
```


# Running storage cli

```
scli log /tmp/fluvio/spu-logs-5001/t1-0/00000000000000000000.log 
```