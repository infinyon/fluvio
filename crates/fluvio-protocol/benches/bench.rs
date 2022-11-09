use std::fs::read;
use std::io::Cursor;

use criterion::{criterion_group, criterion_main, Criterion};

use fluvio_protocol::{Decoder, Encoder, ByteBuf};

const EXAMPLE_WASM_FILE: &str = "fixtures/smartmodule.wasm";

fn bench_encode_vecu8(c: &mut Criterion) {
    let bytes = read(EXAMPLE_WASM_FILE).unwrap();
    let mut dest = vec![];

    c.bench_function("vecu8 encoding", |b| {
        b.iter(|| {
            bytes.encode(&mut dest, 0).unwrap();
        })
    });
}

fn bench_decode_vecu8(c: &mut Criterion) {
    let bytes = read(EXAMPLE_WASM_FILE).unwrap();
    let mut encoded = vec![];

    bytes.encode(&mut encoded, 0).unwrap();

    c.bench_function("vecu8 decoding", |b| {
        b.iter(|| {
            let mut decoded_vecu8: Vec<u8> = vec![];
            let mut cursor = Cursor::new(&encoded);

            decoded_vecu8.decode(&mut cursor, 0).unwrap();
        })
    });
}

fn bench_encode_bytebuf(c: &mut Criterion) {
    let bytes = read(EXAMPLE_WASM_FILE).unwrap();
    let bytebuf: ByteBuf = ByteBuf::from(bytes);

    c.bench_function("bytebuf encoding", |b| {
        b.iter(|| {
            let mut dest = vec![];

            bytebuf.encode(&mut dest, 0).unwrap();
        })
    });
}

fn bench_decode_bytebuf(c: &mut Criterion) {
    let mut encoded: Vec<u8> = vec![];
    let raw: Vec<u8> = read(EXAMPLE_WASM_FILE).unwrap();
    let bytebuf: ByteBuf = ByteBuf::from(raw.to_vec());

    bytebuf.encode(&mut encoded, 0).unwrap();

    c.bench_function("bytebuf decoding", |b| {
        b.iter(|| {
            let mut decoded_bytebuf: ByteBuf = ByteBuf::default();
            let mut cursor = Cursor::new(&encoded);

            decoded_bytebuf.decode(&mut cursor, 0).unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_encode_vecu8,
    bench_decode_vecu8,
    bench_encode_bytebuf,
    bench_decode_bytebuf
);
criterion_main!(benches);
