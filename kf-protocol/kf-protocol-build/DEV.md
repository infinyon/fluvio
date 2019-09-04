# Kafka to Rust code generator

```make build-cli```

You can run code generator CLI (assuming you are at back the root)

```./target/debug/kfspec2code --help```

With Kafka installed in a parallel directory, run file generation

```
../../target/debug/kfspec2code generate -i ../../../kafka/clients/src/main/resources/common/message/ -d ../kf-protocol-message/src/kf_code_gen/
```

