use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use std::time::SystemTime;
use tracing::debug;
use fluvio_test_util::{async_process, fork_and_wait};
use fluvio::{Offset, TopicProducer, TopicProducerConfigBuilder, FluvioAdmin, RecordKey};
use futures::StreamExt;
use std::time::Duration;

use super::DataGeneratorTestCase;
use crate::tests::TestRecordBuilder;

pub async fn producer(
    mut test_driver: TestDriver,
    option: DataGeneratorTestCase,
    producer_id: u32,
    run_id: Option<String>,
) {
    // This is a bit of a hack to prevent attempting to make producer/consumers
    // before the sync topic is created
    async_std::task::sleep(Duration::from_secs(5)).await;

    // Sync topic is unique per instance of generator
    let sync_topic = if let Some(run_id) = &run_id {
        format!("sync-{}", run_id)
    } else {
        format!("sync")
    };

    let test_topic = if let Some(run_id) = &run_id {
        format!("{}-{}", &option.environment.topic_name(), run_id)
    } else {
        format!("{}", &option.environment.topic_name())
    };

    test_driver
        .connect()
        .await
        .expect("Connecting to cluster failed");

    // Create the testing producer

    let maybe_builder = match (option.option.batch_linger_ms, option.option.batch_size) {
        (Some(linger), Some(batch)) => Some(
            TopicProducerConfigBuilder::default()
                .linger(Duration::from_millis(linger))
                .batch_size(batch),
        ),
        (Some(linger), None) => {
            Some(TopicProducerConfigBuilder::default().linger(Duration::from_millis(linger)))
        }
        (None, Some(batch)) => Some(TopicProducerConfigBuilder::default().batch_size(batch)),
        (None, None) => None,
    };

    let producer = if let Some(producer_config) = maybe_builder {
        let config = producer_config.build().expect("producer builder");
        test_driver
            .create_producer_with_config(&test_topic, config)
            .await
    } else {
        test_driver.create_producer(&test_topic).await
    };

    // Create the syncing producer/consumer

    let sync_producer = test_driver.create_producer(&sync_topic).await;
    let sync_consumer = test_driver.get_consumer(&sync_topic, 0).await;

    let mut sync_stream = sync_consumer
        .stream(Offset::from_end(0))
        .await
        .expect("Unable to open stream");

    // Let syncing process know this producer is ready
    sync_producer.send(RecordKey::NULL, "ready").await.unwrap();

    println!("{}: waiting for start", producer_id);
    while let Some(Ok(record)) = sync_stream.next().await {
        //let _key = record
        //    .key()
        //    .map(|key| String::from_utf8_lossy(key).to_string());
        let value = String::from_utf8_lossy(record.value()).to_string();

        if value.eq("start") {
            println!("Starting producer");
            break;
        }
    }

    // Read in the timer value we want to run for
    // Note, we're going to give the consumer a couple extra seconds since it starts its timer first

    let mut records_sent = 0;
    let test_start = SystemTime::now();

    debug!("About to start producer loop");

    if !option.option.runtime_seconds.is_zero() {
        while test_start.elapsed().unwrap() <= option.option.runtime_seconds {
            send_record(&option, producer_id, records_sent, &test_driver, &producer).await;
            records_sent += 1;
        }
    } else {
        loop {
            send_record(&option, producer_id, records_sent, &test_driver, &producer).await;
            records_sent += 1;
        }
    }
    //}

    println!("Producer stopped. Time's up!\nRecords sent: {records_sent}",)
}

async fn send_record(
    option: &DataGeneratorTestCase,
    producer_id: u32,
    records_sent: u32,
    test_driver: &TestDriver,
    producer: &TopicProducer,
) {
    let record = generate_record(option.clone(), producer_id, records_sent);
    test_driver
        .send_count(producer, RecordKey::NULL, record)
        .await
        .expect("Producer Send failed");
    producer.flush().await.expect("flush");
}

fn generate_record(option: DataGeneratorTestCase, producer_id: u32, record_id: u32) -> Vec<u8> {
    let record = TestRecordBuilder::new()
        .with_tag(format!("{}", record_id))
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
            record_id,
            record_json.len(),
            record.crc,
        );
    }
    record_json
}
