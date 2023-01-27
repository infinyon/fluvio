use std::time::SystemTime;
use std::time::Duration;

use futures::FutureExt;
use futures_lite::StreamExt;
use futures::future::try_join_all;
use tokio::select;
use async_channel;
use tracing::info;

use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use fluvio::Offset;
use fluvio_future::timer::sleep;
use fluvio_future::io::Stream;
use fluvio_protocol::record::ConsumerRecord;
use fluvio_protocol::link::ErrorCode;
use crate::tests::TestRecord;

use super::MyTestCase;

// This is for joining multiple topic support per process
async fn consume_from_stream(
    channel: async_channel::Sender<ConsumerRecord>,
    mut stream: impl Stream<Item = Result<ConsumerRecord, ErrorCode>> + Unpin,
) -> Result<(), ()> {
    while let Ok(Some(record_raw)) = stream.try_next().await {
        channel.send(record_raw).await.expect("channel");
    }

    Ok(())
}

// supports multiple topics
pub async fn consumer_stream(test_driver: TestDriver, option: MyTestCase, consumer_id: u32) {
    // Vec of consumer streams
    let mut streams = Vec::new();
    // Create channel here
    let (s, r) = async_channel::bounded(1000);

    // loop over number of topics
    for t in 0..option.environment.topic {
        let topic_name = if option.environment.topic == 1 {
            option.environment.base_topic_name()
        } else {
            format!("{}-{}", option.environment.base_topic_name(), t)
        };

        let consumer = test_driver.get_all_partitions_consumer(&topic_name).await;

        // create a channel
        // pass recv end to function

        // TODO: Support starting stream from consumer offset
        let stream = consumer
            .stream(Offset::from_end(0))
            .await
            .expect("Unable to open stream");

        let shared = Box::pin(consume_from_stream(s.clone(), stream)).shared();

        streams.push(shared);
    }

    // Note, we're going to give the consumer some buffer
    // to give it a better chance to read all records
    let consumer_buffer_time = Duration::from_millis(25);
    let mut test_timer = sleep(option.environment.timeout + consumer_buffer_time);
    let mut records_recvd: i32 = 0;

    let start_consume = SystemTime::now();
    let last_receive = SystemTime::now();
    info!(consumer_id, "Starting consumer");

    'consumer_loop: loop {
        select! {

        _ = &mut test_timer => {
            println!("Consumer {consumer_id} stopped. Time's up!\nRecords received: {records_recvd}");
            break 'consumer_loop
        }

        // Now we just have to listen to the channel
        //stream_next = streams.into_iter().map(|s| s.next()).collect() => {
        _ = try_join_all(streams.clone()) => {}

        // This is for stdout
        record_raw = r.recv() => {
            // Consumer handling code for single stream
            if let Ok(raw) = record_raw {
                records_recvd += 1;

                let result = std::panic::catch_unwind(|| {
                    let record_str = std::str::from_utf8(raw.as_ref()).unwrap();
                    let record_size = record_str.len();
                    let test_record: TestRecord =
                        serde_json::from_str(std::str::from_utf8(raw.as_ref()).unwrap())
                            .expect("Deserialize record failed");

                    //let _consume_latency = now.elapsed().clone().unwrap().as_nanos();

                    if option.option.verbose {
                        println!(
                            "[consumer-{}] record: {:>7} offset: {:>7} (size {:>5}): CRC: {:>10}",
                            consumer_id,
                            records_recvd,
                            raw.offset(),
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
                            raw.offset(),
                        );

                    panic!("Consumer {consumer_id} failed to consume record: {err:?}");
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
