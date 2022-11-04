use std::io::Cursor;

use criterion::{criterion_group, criterion_main, Criterion};

use fluvio_protocol::{Decoder, Encoder};

fn bench_encode_vecu8(c: &mut Criterion) {
    let value: Vec<u8> = vec![0x10, 0x11, 0x12, 0x10, 0x11, 0x12, 0x10, 0x11];
    let mut dest = vec![];

    c.bench_function("encode vecu8", |b| {
        b.iter(|| {
            value.encode(&mut dest, 0).unwrap();
        })
    });
}

fn bench_decode_vecu8(c: &mut Criterion) {
    let encoded_expect: [u8; 14] = [
        0x00, 0x00, 0x00, 0x0A, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
    ];

    c.bench_function("decode vecu8", |b| {
        b.iter(|| {
            let mut decoded_vecu8: Vec<u8> = vec![];

            decoded_vecu8
                .decode(&mut Cursor::new(&encoded_expect), 0)
                .unwrap();
        })
    });
}

criterion_group!(benches, bench_encode_vecu8, bench_decode_vecu8);
criterion_main!(benches);
