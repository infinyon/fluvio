# Building storage cli

From root of the project
```
cargo build --features=cli --manifest-path crates/fluvio-storage/Cargo.toml 
```

# Running storage cli

```
./target/debug/storage-cli log /tmp/fluvio/spu-logs-5001/t1-0/00000000000000000000.log 
```