use fluvio::RecordKey;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;

use super::MyTestCase;

pub async fn producer(test_driver: TestDriver, option: MyTestCase) {
    let producer = test_driver
        .create_producer(&option.environment.base_topic_name())
        .await;

    let iterations: u16 = 10000;
    println!("Producing {iterations} records");
    for i in 0..iterations {
        let record = i.to_string();
        producer.send(RecordKey::NULL, record).await.unwrap();
    }
    producer.flush().await.unwrap();
}
