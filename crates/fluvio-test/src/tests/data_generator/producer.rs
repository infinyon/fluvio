use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use std::time::SystemTime;
use tracing::debug;
use fluvio::{Offset, TopicProducer, TopicProducerConfigBuilder, RecordKey};
use futures::StreamExt;
use std::time::Duration;

use super::GeneratorTestCase;
use crate::tests::TestRecordBuilder;

pub async fn producer(
    mut test_driver: TestDriver,
    option: GeneratorTestCase,
    producer_id: u32,
    run_id: Option<String>,
) {
    // Sync topic is unique per instance of generator
    let sync_topic = if let Some(run_id) = &run_id {
        format!("sync-{}", run_id)
    } else {
        "sync".to_string()
    };

    test_driver
        .connect()
        .await
        .expect("Connecting to cluster failed");

    // Create the testing producer

    // TODO: Create a Vec of producers per topic
    let mut producers = Vec::new();

    for topic_id in 0..option.option.topics {
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

        let env_opts = option.environment.clone();

        let test_topic_name = if let Some(run_id) = run_id.clone() {
            format!("{}-{}-{}", env_opts.topic_name(), run_id, topic_id)
        } else {
            format!("{}-{}", env_opts.topic_name(), topic_id)
        };

        if let Some(producer_config) = maybe_builder {
            let config = producer_config.build().expect("producer builder");
            producers.push(
                test_driver
                    .create_producer_with_config(&test_topic_name, config)
                    .await,
            )
        } else {
            producers.push(test_driver.create_producer(&test_topic_name).await)
        };
    }

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

    let mut records_sent = 0;
    let test_start = SystemTime::now();

    debug!("About to start producer loop");

    if let Some(timeout) = option.option.runtime_seconds {
        while test_start.elapsed().unwrap() <= timeout {
            for producer in producers.iter() {
                send_record(&option, producer_id, records_sent, &test_driver, producer).await;
                records_sent += 1;
            }
        }
    } else {
        loop {
            for producer in producers.iter() {
                send_record(&option, producer_id, records_sent, &test_driver, &producer).await;
                records_sent += 1;
            }
        }
    }

    println!("Producer stopped. Time's up!\nRecords sent: {records_sent}",)
}

async fn send_record(
    option: &GeneratorTestCase,
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

fn generate_record(option: GeneratorTestCase, producer_id: u32, record_id: u32) -> Vec<u8> {
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
