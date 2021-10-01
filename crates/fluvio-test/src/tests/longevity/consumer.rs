use std::time::SystemTime;
use std::time::Duration;

use futures_lite::StreamExt;
use tokio::select;

use fluvio_test_util::test_runner::test_driver::SharedTestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use fluvio::Offset;
use fluvio_future::timer::sleep;

use super::LongevityTestCase;
use super::util::*;

pub async fn consumer_stream(test_driver: SharedTestDriver, option: LongevityTestCase) {
    println!("About to get a consumer");
    let consumer = test_driver
        .get_consumer(&option.environment.topic_name())
        .await;
    println!("About to get a stream");

    // TODO: Support starting stream from consumer offset
    let mut stream = consumer
        .stream(Offset::from_end(0))
        .await
        .expect("Unable to open stream");

    let mut index: i32 = 0;

    // Note, we're going to give the consumer some buffer
    // to give it a better chance to read all records
    let consumer_buffer_time = Duration::from_millis(25);
    let mut test_timer = sleep(option.option.runtime_seconds + consumer_buffer_time);
    let mut records_recvd = 0;

    println!("About to start consumer loop");
    'consumer_loop: loop {
        // Take a timestamp before record consumed
        let now = SystemTime::now();

        select! {

                _ = &mut test_timer => {

                    println!("Consumer stopped. Time's up!\nRecords received: {:?}", records_recvd);
                    break 'consumer_loop
                }

                stream_next = stream.next() => {

                    if let Some(Ok(record_json)) = stream_next {
                        records_recvd += 1;
                        let record: LongevityRecord =
                            serde_json::from_str(std::str::from_utf8(record_json.as_ref()).unwrap())
                                .expect("Deserialize record failed");

                        let _consume_latency = now.elapsed().clone().unwrap().as_nanos();

                        if option.option.verbose {
                            println!(
                                "Consuming {:<7} (size {:<5}): consumed CRC: {:<10}",
                                index,
                                record.data.len(),
                                record.crc,
                            );
                        }

                        assert!(record.validate_crc());

                        //let mut lock = test_driver.write().await;

                        //// record latency
                        //lock.consume_latency_record(consume_latency as u64).await;
                        //lock.consume_bytes_record(record.data.len()).await;

                        //drop(lock);

                        index += 1;
                    } else {
                        panic!("Stream ended unexpectedly")
                    }
                }
        }
    }
}
