use std::sync::Arc;
use std::time::SystemTime;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use futures_lite::StreamExt;
use fluvio::Offset;
use tokio::select;
use std::time::Duration;
use fluvio_future::timer::sleep;

use super::LongevityTestCase;
use super::util::*;

pub async fn consumer_stream(test_driver: Arc<TestDriver>, option: LongevityTestCase) {
    let consumer = test_driver
        .get_consumer(&option.environment.topic_name())
        .await;

    // TODO: Support starting stream from consumer offset
    let mut stream = consumer.stream(Offset::from_end(0)).await.unwrap();

    let mut index: i32 = 0;

    // Note, we're going to give the consumer some buffer since it starts its timer first
    let consumer_buffer_time = Duration::from_millis(10);
    let mut test_timer = sleep(option.option.runtime_seconds + consumer_buffer_time);
    let mut records_recvd = 0;

    'consumer_loop: loop {
        // Take a timestamp before record consumed
        let now = SystemTime::now();

        select! {

                    _ = &mut test_timer => {
                        println!("Records received: {}", records_recvd);
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
