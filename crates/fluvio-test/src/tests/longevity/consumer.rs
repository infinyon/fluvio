use std::time::SystemTime;
use std::time::Duration;

use futures_lite::StreamExt;
use tokio::select;

use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use fluvio::Offset;
use fluvio_future::timer::sleep;
use tracing::info;

use super::LongevityTestCase;
use crate::tests::TestRecord;

pub async fn consumer_stream(test_driver: TestDriver, option: LongevityTestCase, consumer_id: u32) {
    let consumer = test_driver
        .get_consumer(&option.environment.topic_name(), 0)
        .await;

    // TODO: Support starting stream from consumer offset
    let mut stream = consumer
        .stream(Offset::from_end(0))
        .await
        .expect("Unable to open stream");

    // Note, we're going to give the consumer some buffer
    // to give it a better chance to read all records
    let consumer_buffer_time = Duration::from_millis(25);
    let mut test_timer = sleep(option.option.runtime_seconds + consumer_buffer_time);
    let mut records_recvd = 0;

    let start_consume = SystemTime::now();
    let mut last_receive = SystemTime::now();
    info!(consumer_id, "Starting consumer");

    'consumer_loop: loop {
        select! {

                _ = &mut test_timer => {

                    println!("Consumer {consumer_id} stopped. Time's up!\nRecords received: {records_recvd}");
                    break 'consumer_loop
                }

                stream_next = stream.next() => {

                    /*
                    if consumer_id == 1 {
                        panic!("fake test");
                    }
                    */

                    if let Some(Ok(record_raw)) = stream_next {
                        records_recvd += 1;

                        let result = std::panic::catch_unwind(|| {
                            let record_str = std::str::from_utf8(record_raw.as_ref()).unwrap();
                            let record_size = record_str.len();
                            let test_record: TestRecord =
                                serde_json::from_str(std::str::from_utf8(record_raw.as_ref()).unwrap())
                                    .expect("Deserialize record failed");

                            //let _consume_latency = now.elapsed().clone().unwrap().as_nanos();

                            if option.option.verbose {
                                println!(
                                    "[consumer-{}] record: {:>7} offset: {:>7} (size {:>5}): CRC: {:>10}",
                                    consumer_id,
                                    records_recvd,
                                    record_raw.offset(),
                                    record_size,
                                    test_record.crc,
                                );
                            }

                            assert!(test_record.validate_crc());
                        });


                        if let Err(err) = result {
                            let elapsed_time = start_consume.elapsed().unwrap().as_secs();
                            let poll_elapsed_time = last_receive.elapsed().unwrap().as_secs();
                            println!(
                                    "[consumer-{consumer_id}] record: {records_recvd} offset: {}, elapsed: {elapsed_time}  seconds, poll elapsed: {poll_elapsed_time} seconds",
                                    record_raw.offset(),
                                );

                            panic!("Consumer {consumer_id} failed to consume record: {:?}", err);
                        } else {
                            last_receive = SystemTime::now();
                        }


                    } else {
                        let elapsed_time = start_consume.elapsed().unwrap().as_secs();
                        let poll_elapsed_time = last_receive.elapsed().unwrap().as_secs();
                        info!(consumer_id,records_recvd,"stream ended");
                        panic!("{}",format!("Stream ended unexpectedly, consumer: {consumer_id}, records received: {records_recvd}, seconds: {elapsed_time}, poll elapsed: {poll_elapsed_time} seconds"));
                    }
                }
        }
    }
}
