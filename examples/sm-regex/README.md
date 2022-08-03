Example of SmartModule with regex.

To run this, make sure you build CLI at top:
```
$ make build-cli SMARTENGINE=true

```

compile this package:
```
$ make build
```

run regex at top level:

positive:
```
$ target/debug/fluvio sm test --input AA -e regex="[A-Z]" --wasm-file target/wasm32-unknown-unknown/release-lto/fluvio_sm_regex.wasm
1 records
AA
```

negative:
```
$ target/debug/fluvio sm test --input aa -e regex="[A-Z]" --wasm-file target/wasm32-unknown-unknown/release-lto/fluvio_sm_regex.wasm
0 records
```