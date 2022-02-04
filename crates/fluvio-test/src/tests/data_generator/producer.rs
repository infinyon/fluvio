use fluvio::RecordKey;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use std::time::SystemTime;
use tracing::debug;
use fluvio::Offset;
use fluvio_future::io::StreamExt;
use fluvio_future::io::FutureExt;
use fluvio_future::task::spawn;
use fluvio_test_util::{async_process, fork_and_wait};

use std::sync::mpsc::Sender;
use std::sync::mpsc::Receiver;

use super::DataGeneratorTestCase;
use crate::tests::TestRecordBuilder;

pub async fn sync(test_driver: TestDriver, option: DataGeneratorTestCase, trigger: Sender<()>) {
    //debug!("Connect to cluster");
    //            test_driver
    //                .connect()
    //                .await
    //                .expect("Connecting to cluster failed");

    let sync_consumer = test_driver
        .get_consumer(&option.environment.topic_name(), 0)
        .await;

    // TODO: Support starting stream from consumer offset
    let mut sync_stream = sync_consumer
        .stream(Offset::from_end(0))
        .await
        .expect("Unable to open stream");

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

                trigger.send(()).unwrap();

                break 'sync;
            } else {
                println!("[producer] Got: {record:#?}");
            }
        }
    }
}

pub async fn producer(
    mut test_driver: TestDriver,
    option: DataGeneratorTestCase,
    producer_id: u32,
) {
    // Uncommented, this locks
    //let _setup_status = fork_and_wait! {
    //fluvio_future::task::run_block_on(async {

    //    let mut test_driver_sync = test_driver.clone();
    //    let mut sync_opt = option.environment.clone();
    //    debug!("Connect to cluster");
    //        test_driver_sync
    //            .connect()
    //            .await
    //            .expect("Connecting to cluster failed");

    //    //debug!("Create sync topic");
    //    //let mut sync_opt = option.environment.clone();
    //    //sync_opt.topic_name = Some("sync".to_string());
    //    //test_driver.create_topic(&sync_opt).await.unwrap();

    //    let sync_producer = test_driver_sync.create_producer("sync").await;
    //    println!("[producer] about to send ready");
    //    // Send the start command
    //    sync_producer.send(RecordKey::NULL, "ready").await.unwrap();
    //    drop(sync_producer);
    //})};

    debug!("Connect to cluster");
    test_driver
        .connect()
        .await
        .expect("Connecting to cluster failed");

    // Uncommented, this locks
    //let (sync_sender, _sync_receiver) = std::sync::mpsc::channel();
    //let sync_handler = spawn(sync(test_driver.clone(), option.clone(), sync_sender));
    //sync_handler.await;

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

    println!("Producer stopped. Time's up!\nRecords sent: {records_sent}",)
}
