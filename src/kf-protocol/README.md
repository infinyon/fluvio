# kf-protocol

Native Rust implementation of kafka protocol.

This is wrapper for low level kafka protocol crates.

# Testing Client

To test command to test spu server:

```
> ./send-b-client.sh data/apirequest.txt 9004
```

# dump kafka binary as pretty print

First build dump binary:

```cargo build --features="cli"```

Then run binary:
```../target/debug/kafka-dump```



## License

This project is licensed under the [Apache license](LICENSE-APACHE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Fluvio by you, shall be licensed as Apache, without any additional
terms or conditions.
