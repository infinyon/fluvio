use fluvio::RecordKey;
use fluvio_test_util::test_runner::test_driver::{SharedTestDriver};
use fluvio_test_util::test_meta::environment::EnvDetail;

use super::MultiplePartitionTestCase;

pub async fn producer(test_driver: SharedTestDriver, option: MultiplePartitionTestCase) {
    let producer = test_driver
        .create_producer(&option.environment.topic_name())
        .await;

    let iterations: u16 = 5000;
    println!("Producing {} records", iterations);
    for i in 0..iterations {
        let record = i.to_string();
        producer.send(RecordKey::NULL, record).await.unwrap();
    }
}
