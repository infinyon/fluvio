// use std::{time::Duration};

// use async_std::{
//     future::timeout,
//     task::{block_on, spawn},
// };

// use crate::sample::{SampleSendHalf, SampleRecvHalf};

// mod sample;

// fn main() {
//     let num_batches = 2;
//     let num_records_per_batch = 1000;
//     let default_timeout = Duration::from_secs(30);
//     let mut producer_side: Vec<SampleSendHalf> = Vec::with_capacity(num_records_per_batch);
//     let mut consumer_side: Vec<SampleRecvHalf> = Vec::with_capacity(num_records_per_batch);
//     let mut collected_samples: Vec<Duration> =
//         Vec::with_capacity(num_batches * num_records_per_batch);

//     for _ in 0..num_batches {
//         let producer_jh = spawn(timeout(default_timeout, produce_batch(producer_side)));
//         let consumer_jh = spawn(timeout(default_timeout, consume_batch(consumer_side)));
//         producer_side = block_on(producer_jh).expect("Producer timed out").unwrap();
//         consumer_side = block_on(consumer_jh).expect("Consumer timed out").unwrap();

//         assert_eq!(producer_side.len(), num_records_per_batch);
//         assert_eq!(consumer_side.len(), num_records_per_batch);
//         for (p, c) in producer_side.iter().zip(consumer_side.iter()) {
//             assert_eq!(p, c);
//             collected_samples.push(c - p);
//         }
//     }
// }

// async fn produce_batch(producer_side: Vec<SampleSendHalf>) -> Result<Vec<SampleSendHalf>, String> {
//     Ok(producer_side)
// }
// async fn consume_batch(consumer_side: Vec<SampleRecvHalf>) -> Result<Vec<SampleRecvHalf>, String> {
//     Ok(consumer_side)
// }

// fn generate_test_data() -> String {
//     "TODO".to_string()
// }

pub async fn produce() {
    sleep(Duration::from_millis(3)).await
}
pub async fn consume() {
    sleep(Duration::from_millis(3)).await
}

pub const FLUVIO_BENCH_RECORDS_PER_BATCH: &str = "FLUVIO_BENCH_RECORDS_PER_BATCH";
pub const FLUVIO_BENCH_RECORD_NUM_BYTES: &str = "FLUVIO_BENCH_RECORD_NUM_BYTES";
use std::{env, str::FromStr, time::Duration};

use async_std::task::sleep;
pub fn num_records_per_batch() -> usize {
    env_or_default(FLUVIO_BENCH_RECORDS_PER_BATCH, 1000)
}
pub fn record_num_bytes() -> usize {
    env_or_default(FLUVIO_BENCH_RECORDS_PER_BATCH, 1000)
}

fn env_or_default<T: FromStr + std::fmt::Debug>(env_name: &str, default_value: T) -> T
where
    <T as FromStr>::Err: std::fmt::Debug,
{
    env::var(env_name)
        .map(|s| s.parse::<T>().unwrap())
        .unwrap_or(default_value)
}
