use async_std::{task::spawn, future::timeout, prelude::FutureExt};
use criterion::black_box;

use crate::{Setup, DEFAULT_TIMEOUT};

/// Spawns two async threads (one for producer and one for consumer), waits for them to complete and then joins them.
/// Consumer thread returns when it has read all expected messages
pub async fn run_throughput_test(setup: Setup) {
    let (producer, consumer) = setup;
    // producer.produce().await;
    // consumer.consume().await;
    let producer_jh = spawn(timeout(DEFAULT_TIMEOUT, producer.produce()));
    let consumer_jh = spawn(timeout(DEFAULT_TIMEOUT, consumer.consume()));
    let (producer_result, consumer_result) = producer_jh.join(consumer_jh).await;
    producer_result.unwrap();
    consumer_result.unwrap();
}

pub async fn run_throughput_no_check(setup: Setup) {
    let (producer, consumer) = setup;
    // producer.produce().await;
    // consumer.consume().await;
    let producer_jh = spawn(timeout(DEFAULT_TIMEOUT, producer.produce()));
    let consumer_jh = spawn(timeout(DEFAULT_TIMEOUT, consumer.consume()));
    let (producer_result, consumer_result) = producer_jh.join(consumer_jh).await;
    producer_result.unwrap();
    consumer_result.unwrap();
}

/// Tests the overhead of the benchmark async framework
pub async fn run_overhead_test(_: Setup) {
    let producer_jh = spawn(timeout(DEFAULT_TIMEOUT, nop()));
    let consumer_jh = spawn(timeout(DEFAULT_TIMEOUT, nop()));
    let (producer_result, consumer_result) = producer_jh.join(consumer_jh).await;
    producer_result.unwrap();
    consumer_result.unwrap();
}

pub async fn run_producer_test(setup: Setup) {
    let (producer, _) = setup;
    producer.produce().await;
}

async fn nop() {
    black_box(())
}
