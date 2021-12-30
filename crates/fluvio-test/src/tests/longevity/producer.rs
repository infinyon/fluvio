use fluvio::RecordKey;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use std::time::SystemTime;
use tracing::debug;

use super::LongevityTestCase;
use crate::tests::TestRecordBuilder;

pub async fn producer(test_driver: TestDriver, option: LongevityTestCase, producer_id: u32) {
    debug!("About to get a producer");
    let producer = test_driver
        .create_producer(&option.environment.topic_name())
        .await;

    // Read in the timer value we want to run for
    // Note, we're going to give the consumer a couple extra seconds since it starts its timer first

    let mut records_sent = 0;
    let test_start = SystemTime::now();

    debug!("About to start producer loop");
    while test_start.elapsed().unwrap() <= option.option.runtime_seconds {
        let record = TestRecordBuilder::new()
            .with_tag(format!("{}", records_sent))
            .with_random_data(option.option.record_size)
            .build();
        let record_json = serde_json::to_string(&record)
            .expect("Convert record to json string failed")
            .as_bytes()
            .to_vec();

        debug!("{:?}", &record);

        if option.option.verbose {
            println!(
                "[producer-{}] record: {:>7} (size {:>5}): CRC: {:>10}",
                producer_id,
                records_sent,
                record_json.len(),
                record.crc,
            );
        }

        // Record the latency
        test_driver
            .send_count(&producer, RecordKey::NULL, record_json)
            .await
            .expect("Producer Send failed");
        producer.flush().await.expect("flush");

        records_sent += 1;
    }
    //}

    println!(
        "Producer stopped. Time's up!\nRecords sent: {:?}",
        records_sent
    )
}
