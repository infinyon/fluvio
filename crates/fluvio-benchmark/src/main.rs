use std::{time::Duration};

use async_std::{
    future::timeout,
    task::{block_on, spawn},
};

use crate::sample::{SampleSendHalf, SampleRecvHalf};

mod sample;

fn main() {
    let num_batches = 2;
    let num_records_per_batch = 1000;
    let default_timeout = Duration::from_secs(30);
    let mut producer_side: Vec<SampleSendHalf> = Vec::with_capacity(num_records_per_batch);
    let mut consumer_side: Vec<SampleRecvHalf> = Vec::with_capacity(num_records_per_batch);
    let mut collected_samples: Vec<Duration> =
        Vec::with_capacity(num_batches * num_records_per_batch);

    for _ in 0..num_batches {
        let producer_jh = spawn(timeout(default_timeout, produce_batch(producer_side)));
        let consumer_jh = spawn(timeout(default_timeout, consume_batch(consumer_side)));
        producer_side = block_on(producer_jh).expect("Producer timed out").unwrap();
        consumer_side = block_on(consumer_jh).expect("Consumer timed out").unwrap();

        assert_eq!(producer_side.len(), num_records_per_batch);
        assert_eq!(consumer_side.len(), num_records_per_batch);
        for (p, c) in producer_side.iter().zip(consumer_side.iter()) {
            assert_eq!(p, c);
            collected_samples.push(c - p);
        }
    }
}

async fn produce_batch(producer_side: Vec<SampleSendHalf>) -> Result<Vec<SampleSendHalf>, String> {
    Ok(producer_side)
}
async fn consume_batch(consumer_side: Vec<SampleRecvHalf>) -> Result<Vec<SampleRecvHalf>, String> {
    Ok(consumer_side)
}

fn generate_test_data() -> String {
    "TODO".to_string()
}
