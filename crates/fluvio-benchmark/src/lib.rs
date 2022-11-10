use std::time::Duration;

use async_std::{
    task::{block_on, spawn},
    future::timeout,
    prelude::FutureExt,
    stream::StreamExt,
};
use bench_env::{FLUVIO_BENCH_RECORDS_PER_BATCH, EnvOrDefault, FLUVIO_BENCH_RECORD_NUM_BYTES};
use fluvio::{
    TopicProducer, PartitionConsumer, metadata::topic::TopicSpec, FluvioAdmin, RecordKey, Offset,
};
use rand::{distributions::Alphanumeric, Rng};

pub mod bench_env;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const TOPIC_NAME: &str = "benchmarking-topic";

pub type Setup = (
    (TopicProducer, std::vec::Vec<std::string::String>),
    (PartitionConsumer, std::vec::Vec<std::string::String>),
);

pub async fn produce(producer: TopicProducer, producer_data: Vec<String>) {
    for record_data in producer_data {
        producer.send(RecordKey::NULL, record_data).await.unwrap();
    }
    producer.flush().await.unwrap();
}

pub async fn consume(consumer: PartitionConsumer, consumer_data: Vec<String>) {
    let mut stream = consumer.stream(Offset::absolute(0).unwrap()).await.unwrap();

    for expected in consumer_data {
        let record = stream.next().await.unwrap().unwrap();
        let value = String::from_utf8_lossy(record.value());
        assert_eq!(value, expected);
    }
}

pub fn setup() -> Setup {
    block_on(timeout(DEFAULT_TIMEOUT, do_setup())).unwrap()
}

pub async fn do_setup() -> Setup {
    // println!("Setting up for new iteration");
    let new_topic = TopicSpec::new_computed(1, 1, None);

    // println!("Connecting to fluvio");

    let admin = FluvioAdmin::connect().await.unwrap();
    // println!("Deleting old topic if present");
    let _ = admin
        .delete::<TopicSpec, String>(TOPIC_NAME.to_string())
        .await;
    // println!("Creating new topic {TOPIC_NAME}");
    admin
        .create(TOPIC_NAME.to_string(), false, new_topic)
        .await
        .unwrap();

    // println!("Creating producer and consumers");
    let producer = fluvio::producer(TOPIC_NAME).await.unwrap();
    let consumer = fluvio::consumer(TOPIC_NAME, 0).await.unwrap();
    let data: Vec<String> = (0..FLUVIO_BENCH_RECORDS_PER_BATCH.env_or_default())
        .map(|_| generate_random_string(FLUVIO_BENCH_RECORD_NUM_BYTES.env_or_default()))
        .collect();

    ((producer, data.clone()), (consumer, data))
}

pub async fn run_test(setup: Setup) {
    let ((producer, producer_data), (consumer, consumer_data)) = setup;
    let producer_jh = spawn(timeout(DEFAULT_TIMEOUT, produce(producer, producer_data)));
    let consumer_jh = spawn(timeout(DEFAULT_TIMEOUT, consume(consumer, consumer_data)));
    let (producer_result, consumer_result) = producer_jh.join(consumer_jh).await;
    producer_result.unwrap();
    consumer_result.unwrap();
}

fn generate_random_string(size: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .map(char::from)
        .collect()
}
