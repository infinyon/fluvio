use criterion::*;
use fluvio_benchmark::{
    setup,
    bench_env::{
        FLUVIO_BENCH_RECORD_NUM_BYTES, EnvOrDefault, FLUVIO_BENCH_RECORDS_PER_BATCH,
        FLUVIO_BENCH_SAMPLE_SIZE,
    },
    throughput::run_throughput_test,
};

use criterion::async_executor::AsyncStdExecutor;

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput-example");

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
// fn bench_latency(c: &mut Criterion) {
//     let mut group = c.benchmark_group("throughput-example");

//     group.throughput(Throughput::Bytes(
//         FLUVIO_BENCH_RECORD_NUM_BYTES.env_or_default() as u64,
//     ));
//     group.bench_function("produce and consume single", |b| {
//         b.to_async(AsyncStdExecutor).iter_batched(
//             || setup(),
//             |o| async move { run_latency_test(o).await },
//             BatchSize::PerIteration,
//         )
//     });
//     group.finish();
// }

criterion_group! {
    name = benches;
    // This can be any expression that returns a `Criterion` object.
    config = Criterion::default().sample_size(FLUVIO_BENCH_SAMPLE_SIZE.env_or_default());
    targets = bench_throughput
}
criterion_main!(benches);
