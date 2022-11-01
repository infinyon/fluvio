#![feature(test)]

extern crate test;

#[cfg(test)]
mod bench {
    use std::io::Cursor;

    use fluvio_protocol::{Decoder, Encoder};
    use test::{Bencher, black_box};

    #[bench]
    fn bench_encode_vecu8(b: &mut Bencher) {
        let value: Vec<u8> = vec![0x10, 0x11, 0x12, 0x10, 0x11, 0x12, 0x10, 0x11];

        b.iter(|| {
            for i in 1..100 {
                let mut dest = vec![];

                black_box(|| {
                    value.encode(&mut dest, 0);
                });
            }
        });
    }

    #[bench]
    fn bench_decode_vecu8(b: &mut Bencher) {
        let encoded_expect: [u8; 14] = [
            0x00, 0x00, 0x00, 0x0A, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
        ];

        b.iter(|| {
            for i in 1..100 {
                let mut decoded_vecu8: Vec<u8> = vec![];

                black_box(|| {
                    decoded_vecu8.decode(&mut Cursor::new(&encoded_expect), 0);
                });
            }
        });
    }
}
