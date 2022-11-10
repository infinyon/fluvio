use std::time::Duration;

use async_std::{
    task::{spawn, sleep},
    future::timeout,
    prelude::FutureExt,
};
use criterion::*;
use fluvio_benchmark::{consume, produce};

use criterion::async_executor::AsyncStdExecutor;

fn setup() -> usize {
    5
}

async fn run_test(e: usize) {
    let default_timeout = Duration::from_millis(10);
    let producer_jh = spawn(timeout(default_timeout, produce()));
    let consumer_jh = spawn(timeout(default_timeout, consume()));
    let (producer_result, consumer_result) = producer_jh.join(consumer_jh).await;
    producer_result.unwrap();
    consumer_result.unwrap();
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput-example");

    let records = ["abc", "bcd", "cde"];
    // TODO throughput
    group.throughput(Throughput::Bytes(9 as u64)); // TODO
    group.bench_function("produce and consume", |b| {
        b.to_async(AsyncStdExecutor).iter_batched(
            || setup(),
            |o| async move { run_test(o).await },
            BatchSize::PerIteration,
        )
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
