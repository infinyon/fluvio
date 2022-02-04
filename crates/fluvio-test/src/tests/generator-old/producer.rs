use structopt::StructOpt;
use uuid::Uuid;
use tracing::debug;
use futures_lite::StreamExt;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::{EnvironmentSetup};
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::async_process;
use super::GeneratorTestCase;
use fluvio_future::task::run_block_on;
use fluvio::Offset;
use fluvio_test_util::test_meta::environment::EnvDetail;
use fluvio::{RecordKey};

pub async fn producer_proc(
    mut test_driver: TestDriver,
    option: GeneratorTestCase,
    producer_uuid: Uuid,
    parent_uuid: Uuid,
) {
    // TODO: Wait a moment to allow the driver to create topics
    println!("[producer] Producer sleeping for a moment");
    println!("[producer] Generate data here");
    std::thread::sleep(std::time::Duration::from_secs(10));

    println!("[producer] Producer done sleeping");

    let parent_uuid = parent_uuid.clone();

    let producer_id = producer_uuid;
    println!("[producer] Producer #{producer_id} starting test");

    //producer::producer(test_driver, option, i).await

    let sync_topic = if !option.option.debug {
        format!("{}-{}", option.option.prefix_sync_topic, parent_uuid)
    } else {
        format!("{}", option.option.prefix_sync_topic)
    };

    println!("[producer] sync consumer");

    let sync_consumer = test_driver.get_consumer(&sync_topic, 0).await;

    // TODO: Support starting stream from consumer offset
    let mut sync_stream = sync_consumer
        .stream(Offset::from_end(0))
        .await
        .expect("Unable to open stream");

    //let command_consumer = test_driver.get_consumer(&option.option.prefix_command_topic, 0).await;

    println!("[producer] sync producer");
    let producer = test_driver.create_producer("sync").await;

    println!("[producer] about to send ready");
    // Send the start command
    producer.send(RecordKey::NULL, "ready").await.unwrap();
    drop(producer);

    println!("[producer] start loop");
    // Sync with driver
    'sync: loop {
        if let Some(Ok(record_raw)) = sync_stream.next().await {
            let record: Vec<&str> = std::str::from_utf8(record_raw.as_ref())
                .unwrap()
                .split_whitespace()
                .collect();

            if record[0] == "start" {
                println!("[producer] Got the start signal!");
                break 'sync;
            } else {
                println!("[producer] Got: {record:#?}");
            }
        }
    }

    // Move this into the runner
    //let config = if self.interactive_mode() {
    //    TopicProducerConfigBuilder::default()
    //        .linger(std::time::Duration::from_millis(10))
    //        .build()
    //        .map_err(FluvioError::from)?
    //} else {
    //    Default::default()
    //};
    //let producer = fluvio
    //    .topic_producer_with_config(&self.topic, config)
    //    .await?;

    println!("Producer #{producer_id} ending test");
}
