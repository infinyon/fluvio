# Test Client

Test Client command to test spu server:

```
> ./send-b-client.sh data/apirequest.txt 9004
```

# dump kafka binary as pretty print

First build dump binary:

```cargo build --features="cli"```

Then run binary:
```../target/debug/kafka-dump```
