use std::sync::mpsc::Sender;

use fluvio::RecordKey;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use tracing::{instrument, Instrument, debug_span};

use super::ConcurrentTestCase;
use super::util::*;

#[instrument(skip(test_driver))]
pub async fn producer(
    test_driver: &TestDriver,
    option: ConcurrentTestCase,
    digests: Sender<String>,
) {
    let producer = test_driver
        .create_producer(&option.environment.base_topic_name())
        .instrument(debug_span!("producer_create"))
        .await;

    // Iterations ranging approx. 5000 - 20_000
    let iterations: u16 = (rand::random::<u16>() / 2) + 20000;
    println!("Producing {} records", iterations);
    for i in 0..iterations {
        let record = rand_record();
        let record_digest = hash_record(&record);
        digests.send(record_digest).unwrap();
        producer
            .send(RecordKey::NULL, record)
            .instrument(debug_span!("producer_send", i = i))
            .await
            .unwrap();
    }
    producer
        .flush()
        .instrument(debug_span!("producer_flush"))
        .await
        .unwrap();
}
