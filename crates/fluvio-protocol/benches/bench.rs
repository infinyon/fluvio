#![feature(test)]

extern crate test;

#[cfg(test)]
mod bench {
  use test::{Bencher, black_box};

  #[bench]
  fn bench_pow(b: &mut Bencher) {
    // Optionally include some setup
    let x: f64 = 211.0 * 11.0;
    let y: f64 = 301.0 * 103.0;

    b.iter(|| {
        // Inner closure, the actual test
        for i in 1..100 {
            black_box(x.powf(y).powf(x));
        }
    });
  }
}
