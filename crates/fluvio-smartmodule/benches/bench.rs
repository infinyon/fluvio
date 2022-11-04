use std::fs::read;

use criterion::{criterion_group, criterion_main, Criterion};

use fluvio_protocol::Encoder;
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

const EXAMPLE_WASM_FILE: &str = "fixtures/smartmodule.wasm";

fn bench_encode_smartmodule_encode(c: &mut Criterion) {
    let wasm_bytes: Vec<u8> = read(EXAMPLE_WASM_FILE).unwrap();

    c.bench_function("encode wasm file", |b| {
        let smartmodule = SmartModuleInput::new(wasm_bytes.clone(), 0);
        let mut dest = vec![];

        b.iter(|| {
            smartmodule.encode(&mut dest, 0).unwrap();
        })
    });
}

criterion_group!(benches, bench_encode_smartmodule_encode);
criterion_main!(benches);
