use fluvio::{Fluvio, Offset};
use futures_lite::StreamExt;
use fluvio_future::task::spawn;
use md5::Digest;
use std::sync::mpsc::{Receiver, Sender};
use fluvio_system_util::bin::get_fluvio;
use fluvio_command::CommandExt;
use crate::TestOption;

const TOPIC: &str = "test-bug";
const PARTITION: i32 = 0;

type Record = Vec<u8>;

pub async fn test_concurrent_consume_produce(option: &TestOption) {
    println!("Testing concurrent consumer and producer");
    setup_produce_then_consume(option);
    let (sender, receiver) = std::sync::mpsc::channel();
    spawn(consumer_stream(receiver));
    producer(sender).await;
}

fn setup_produce_then_consume(option: &TestOption) {
    let mut command = get_fluvio().expect("fluvio not founded");
    command
        .arg("topic")
        .arg("create")
        .arg(TOPIC)
        .arg("--replication")
        .arg(option.replication().to_string());
    if let Some(log) = &option.client_log {
        command.env("RUST_LOG", log);
    }

    let _output = command
        .result()
        .expect("fluvio topic create should succeed");
}

async fn consumer_stream(digests: Receiver<String>) {
    let fluvio = Fluvio::connect().await.unwrap();
    let consumer = fluvio.partition_consumer(TOPIC, PARTITION).await.unwrap();
    let mut stream = consumer.stream(Offset::beginning()).await.unwrap();

    let mut index: i32 = 0;
    while let Some(Ok(record)) = stream.next().await {
        let existing_record_digest = digests.recv().unwrap();
        let current_record_digest = hash_record(record.as_ref());
        println!(
            "Consuming {:<5} (size {:<5}): was produced: {}, was consumed: {}",
            index,
            record.as_ref().len(),
            existing_record_digest,
            current_record_digest
        );
        assert_eq!(existing_record_digest, current_record_digest);
        index += 1;
    }
}

async fn producer(digests: Sender<String>) {
    let fluvio = Fluvio::connect().await.unwrap();
    let producer = fluvio.topic_producer(TOPIC).await.unwrap();

    // Iterations ranging approx. 5000 - 20_000
    let iterations: u16 = (rand::random::<u16>() / 2) + 20000;
    println!("Producing {} records", iterations);
    for _ in 0..iterations {
        let record = rand_record();
        let record_digest = hash_record(&record);
        digests.send(record_digest).unwrap();
        producer.send_record(&record, PARTITION).await.unwrap();
    }
}

fn rand_record() -> Record {
    let len: u16 = rand::random();
    let record: Vec<u8> = (0..len).map(|_| rand::random::<u8>()).collect();
    record
}

fn hash_messages(messages: &[String]) -> String {
    let mut hasher = md5::Md5::new();
    for m in messages.iter() {
        hasher.update(m);
    }
    format!("{:X?}", hasher.finalize())
}

fn hash_record(record: &[u8]) -> String {
    format!("{:X}", md5::Md5::digest(record))
}
