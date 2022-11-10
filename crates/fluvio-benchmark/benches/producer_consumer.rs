use criterion::*;
use fluvio_benchmark::{
    setup, run_test,
    bench_env::{FLUVIO_BENCH_RECORD_NUM_BYTES, EnvOrDefault, FLUVIO_BENCH_RECORDS_PER_BATCH},
};

use criterion::async_executor::AsyncStdExecutor;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput-example");

    // TODO throughput
    group.throughput(Throughput::Bytes(
        (FLUVIO_BENCH_RECORD_NUM_BYTES.env_or_default()
            * FLUVIO_BENCH_RECORDS_PER_BATCH.env_or_default()) as u64,
    )); // TODO
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
