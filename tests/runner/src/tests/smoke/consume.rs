// test consumer

use std::{io, time::Duration};
use std::io::Write;
use std::collections::HashMap;

use log::{info, debug};
use futures_lite::stream::StreamExt;

use fluvio_system_util::bin::get_fluvio;
use fluvio::{Fluvio, Offset, PartitionConsumer};
use fluvio_command::CommandExt;

use crate::cli::TestOption;
use super::message::*;

type Offsets = HashMap<String, i64>;

/// total time allotted for consumer test
fn consume_wait_timeout() -> u64 {
    let var_value = std::env::var("FLV_TEST_CONSUMER_WAIT").unwrap_or_default();
    var_value.parse().unwrap_or(15000) // 15 seconds
}

/// verify consumers
pub async fn validate_consume_message(option: &TestOption, offsets: Offsets) {
    if option.use_cli() {
        validate_consume_message_cli(option, offsets);
    } else {
        validate_consume_message_api(offsets, option).await;
    }
}

fn validate_consume_message_cli(option: &TestOption, offsets: Offsets) {
    let replication = option.replication();

    for i in 0..replication {
        let topic_name = option.topic_name(i);
        let offset = offsets.get(&topic_name).expect("topic offset");
        let mut command = get_fluvio().expect("fluvio not found");
        command
            .arg("consume")
            .arg(&topic_name)
            .arg("--partition")
            .arg("0")
            .arg("-d")
            .arg("-o")
            .arg(offset.to_string());
        println!("Executing> {}", command.display());
        let output = command.result().expect("fluvio command failed");

        io::stderr().write_all(&output.stderr).unwrap();

        let msg = output.stdout.as_slice();
        validate_message(i, *offset, &topic_name, option, &msg[0..msg.len() - 1]);

        println!("topic: {}, consume message validated!", topic_name);
    }
}
async fn get_consumer(client: &Fluvio, topic: &str) -> PartitionConsumer {
    use fluvio_future::timer::sleep;

    for _ in 0..10 {
        match client.partition_consumer(topic.to_string(), 0).await {
            Ok(client) => return client,
            Err(err) => {
                println!(
                    "unable to get consumer to topic: {}, error: {} sleeping 10 second ",
                    topic, err
                );
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    panic!("can't get consumer");
}

async fn validate_consume_message_api(offsets: Offsets, option: &TestOption) {
    use tokio::select;
    use fluvio_future::timer::sleep;

    let client = Fluvio::connect().await.expect("should connect");
    let replication = option.replication();
    let iteration = option.produce.produce_iteration;

    for i in 0..replication {
        let topic_name = option.topic_name(i);
        let base_offset = offsets.get(&topic_name).expect("offsets");
        println!(
            "starting fetch stream for: {} base offset: {}, expected new records: {}",
            topic_name, base_offset, iteration
        );

        let consumer = get_consumer(&client, &topic_name).await;

        let mut stream = consumer
            .stream(
                Offset::absolute(*base_offset)
                    .unwrap_or_else(|_| panic!("creating stream for iteration: {}", i)),
            )
            .await
            .expect("stream");

        let mut total_records: u16 = 0;

        let mut timer = sleep(Duration::from_millis(consume_wait_timeout()));

        loop {
            select! {

                _ = &mut timer => {
                    debug!("timer expired");
                    panic!("timer expired");
                },

                // max time for each read
                _ = sleep(Duration::from_millis(5000)) => {
                    panic!("no consumer read iter: current {}",iteration);
                },

                stream_next = stream.next() => {

                    if let Some(Ok(record)) = stream_next {

                        let offset = record.offset();
                        let bytes = record.as_ref();
                        info!(
                            "* consumer iter: {}, received offset: {}, bytes: {}",
                            total_records,
                            offset,
                            bytes.len()
                        );
                        validate_message(iteration, offset, &topic_name, option, &bytes);
                        info!(
                            " total records: {}, validated offset: {}",
                            total_records, offset
                        );
                        total_records += 1;
                        if total_records == iteration {
                            println!("<<consume test done for: {} >>>>", topic_name);
                            println!("consume message validated!, records: {}",total_records);
                            break;
                        }

                    } else {
                        panic!("no more stream");
                    }
                }

            }
        }
    }
}
