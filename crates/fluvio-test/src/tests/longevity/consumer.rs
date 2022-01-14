use std::time::SystemTime;
use std::time::Duration;

use futures_lite::StreamExt;
use tokio::select;

use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use fluvio::Offset;
use fluvio_future::timer::sleep;

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

    'consumer_loop: loop {
        // Take a timestamp before record consumed
        let now = SystemTime::now();

        select! {

                _ = &mut test_timer => {

                    println!("Consumer {consumer_id} stopped. Time's up!\nRecords received: {records_recvd}");
                    break 'consumer_loop
                }

                stream_next = stream.next() => {

                    if let Some(Ok(record_raw)) = stream_next {
                        records_recvd += 1;

                        let record_str = std::str::from_utf8(record_raw.as_ref()).unwrap();
                        let record_size = record_str.len();
                        let test_record: TestRecord =
                            serde_json::from_str(std::str::from_utf8(record_raw.as_ref()).unwrap())
                                .expect("Deserialize record failed");

                        let _consume_latency = now.elapsed().clone().unwrap().as_nanos();

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

                    //    let elapsed_time = now.elapsed().unwrap().as_secs();

                    } else {
                        let elapsed_time = now.elapsed().unwrap().as_secs();
                        panic!("{}",format!("Stream ended unexpectedly, consumer: {consumer_id}, records received: {records_recvd}, seconds: {elapsed_time}"));
                    }
                }
        }
    }
}
