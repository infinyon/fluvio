use criterion::*;
use fluvio_benchmark::{
    setup,
    bench_env::{
        FLUVIO_BENCH_RECORD_NUM_BYTES, EnvOrDefault, FLUVIO_BENCH_RECORDS_PER_BATCH,
        FLUVIO_BENCH_SAMPLE_SIZE, FLUVIO_BENCH_MAX_BYTES_PER_BATCH,
    },
    benches::{run_throughput_test, run_overhead_test, run_throughput_no_check},
};

use criterion::async_executor::AsyncStdExecutor;

fn bench_throughput(c: &mut Criterion) {
    println!("Test configuration:");
    println!(
        "{}:{}",
        FLUVIO_BENCH_RECORDS_PER_BATCH.0,
        FLUVIO_BENCH_RECORDS_PER_BATCH.env_or_default()
    );
    println!(
        "{}:{}",
        FLUVIO_BENCH_RECORD_NUM_BYTES.0,
        FLUVIO_BENCH_RECORD_NUM_BYTES.env_or_default()
    );
    println!(
        "{}:{}",
        FLUVIO_BENCH_MAX_BYTES_PER_BATCH.0,
        FLUVIO_BENCH_MAX_BYTES_PER_BATCH.env_or_default()
    );
    let mut group = c.benchmark_group("throughput");

    group.throughput(Throughput::Bytes(
        (FLUVIO_BENCH_RECORD_NUM_BYTES.env_or_default()
            * FLUVIO_BENCH_RECORDS_PER_BATCH.env_or_default()) as u64,
    ));
    group.bench_function("produce and consume batch", |b| {
        b.to_async(AsyncStdExecutor).iter_batched(
            || setup(),
            |o| async move { run_throughput_test(o).await },
            BatchSize::PerIteration,
        )
    });
    group.finish();
}

fn bench_throughput_no_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput no check");

    group.throughput(Throughput::Bytes(
        (FLUVIO_BENCH_RECORD_NUM_BYTES.env_or_default()
            * FLUVIO_BENCH_RECORDS_PER_BATCH.env_or_default()) as u64,
    ));
    group.bench_function("produce and consume batch", |b| {
        b.to_async(AsyncStdExecutor).iter_batched(
            || setup(),
            |o| async move { run_throughput_no_check(o).await },
            BatchSize::PerIteration,
        )
    });
    group.finish();
}

fn bench_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("overhead");

    group.bench_function("check overhead", |b| {
        b.to_async(AsyncStdExecutor).iter_batched(
            || setup(),
            |o| async move { run_overhead_test(o).await },
            BatchSize::PerIteration,
        )
    });
    group.finish();
}

criterion_group! {
    name = benches;
    // This can be any expression that returns a `Criterion` object.
    config = Criterion::default().sample_size(FLUVIO_BENCH_SAMPLE_SIZE.env_or_default());
    targets = bench_overhead, bench_throughput,  bench_throughput_no_check
}
criterion_main!(benches);
