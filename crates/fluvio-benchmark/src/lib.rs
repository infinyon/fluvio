use std::{time::Duration, collections::VecDeque};

use async_std::{task::block_on, future::timeout, stream::StreamExt};
use bench_env::{
    FLUVIO_BENCH_RECORDS_PER_BATCH, EnvOrDefault, FLUVIO_BENCH_RECORD_NUM_BYTES,
    FLUVIO_BENCH_MAX_BYTES_PER_BATCH,
};
use consumer::Consumer;
use fluvio::{
    metadata::topic::TopicSpec, FluvioAdmin, RecordKey, Offset, TopicProducerConfigBuilder, Fluvio,
};
use producer::Producer;
use rand::{distributions::Alphanumeric, Rng};

pub mod bench_env;
pub mod benches;
pub mod producer;
pub mod consumer;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
const TOPIC_NAME: &str = "benchmarking-topic";

pub type Setup = (Producer, Consumer);

pub fn setup() -> Setup {
    block_on(timeout(DEFAULT_TIMEOUT, do_setup())).unwrap()
}

pub async fn do_setup() -> Setup {
    let new_topic = TopicSpec::new_computed(1, 1, None);

    let admin = FluvioAdmin::connect().await.unwrap();
    let _ = admin
        .delete::<TopicSpec, String>(TOPIC_NAME.to_string())
        .await;
    admin
        .create(TOPIC_NAME.to_string(), false, new_topic)
        .await
        .unwrap();

    let fluvio = Fluvio::connect().await.unwrap();
    let config = TopicProducerConfigBuilder::default()
        .batch_size(FLUVIO_BENCH_MAX_BYTES_PER_BATCH.env_or_default())
        .build()
        .unwrap();
    let producer = fluvio
        .topic_producer_with_config(TOPIC_NAME, config)
        .await
        .unwrap();
    let consumer = fluvio::consumer(TOPIC_NAME, 0).await.unwrap();
    let data: VecDeque<String> = (0..FLUVIO_BENCH_RECORDS_PER_BATCH.env_or_default())
        .map(|_| generate_random_string(FLUVIO_BENCH_RECORD_NUM_BYTES.env_or_default()))
        .collect();

    // Send and Retrieve
    producer.send(RecordKey::NULL, "setup").await.unwrap();
    producer.flush().await.unwrap();
    consumer
        .stream(Offset::absolute(0).unwrap())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();

    (
        Producer {
            producer,
            data: data.clone(),
            records_per_batch: FLUVIO_BENCH_RECORDS_PER_BATCH.env_or_default(),
        },
        Consumer {
            consumer,
            data,
            // one because we already sent and consumed one as part of setup
            offset: 1,
            records_per_batch: FLUVIO_BENCH_RECORDS_PER_BATCH.env_or_default(),
        },
    )
}

fn generate_random_string(size: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .map(char::from)
        .collect()
}
