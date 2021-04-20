use std::sync::Arc;
use std::sync::mpsc::Sender;
use fluvio::Fluvio;

use super::ConcurrentTestCase;
use super::util::*;

pub async fn producer(fluvio: Arc<Fluvio>, option: ConcurrentTestCase, digests: Sender<String>) {
    let producer = fluvio
        .topic_producer(option.environment.topic_name.clone())
        .await
        .unwrap();

    // Iterations ranging approx. 5000 - 20_000
    let iterations: u16 = (rand::random::<u16>() / 2) + 20000;
    println!("Producing {} records", iterations);
    for _ in 0..iterations {
        let record = rand_record();
        let record_digest = hash_record(&record);
        digests.send(record_digest).unwrap();
        producer.send_record(record, 0).await.unwrap();
    }
}
