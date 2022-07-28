use fluvio::RecordKey;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use tracing::{instrument, Instrument, debug_span};

use super::MultiplePartitionTestCase;

#[instrument(skip(test_driver))]
pub async fn producer(test_driver: TestDriver, option: MultiplePartitionTestCase) {
    let producer = test_driver
        .create_producer(&option.environment.base_topic_name())
        .instrument(debug_span!("producer_create"))
        .await;

    let iterations: u16 = 10000;
    println!("Producing {} records", iterations);
    for i in 0..iterations {
        let record = i.to_string();
        producer
            .send(RecordKey::NULL, record)
            .instrument(debug_span!("producer_send", record_num = i))
            .await
            .unwrap();
    }
    producer
        .flush()
        .instrument(debug_span!("producer_flush"))
        .await
        .unwrap();
}
