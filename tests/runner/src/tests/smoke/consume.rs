// test consumer

use std::io;
use std::io::Write;
use std::collections::HashMap;

use log::info;
use futures_lite::stream::StreamExt;
use async_channel::{bounded, Sender};
use fluvio_system_util::bin::get_fluvio;
use fluvio::{Fluvio, Offset};
use crate::cli::TestOption;
use crate::util::CommandUtil;
use super::message::*;

type Offsets = HashMap<String, i64>;

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
        let output = get_fluvio()
            .expect("fluvio not founded")
            .arg("consume")
            .arg(&topic_name)
            .arg("--partition")
            .arg("0")
            .arg("-d")
            .arg("-o")
            .arg(offset.to_string())
            .print()
            .output()
            .expect("no output");

        // io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();

        let msg = output.stdout.as_slice();
        validate_message(i, *offset, &topic_name, option, &msg[0..msg.len() - 1]);

        println!("topic: {}, consume message validated!", topic_name);
    }
}

async fn validate_consume_message_api(offsets: Offsets, option: &TestOption) {
    use std::time::Duration;
    use std::collections::HashSet;
    use tokio::select;
    use fluvio_future::task::spawn;
    use fluvio_future::timer::sleep;

    let mut async_consumers = HashSet::new();
    let (sender, mut receiver) = bounded::<String>(5);
    for i in 0..option.topics() {
        let topic = option.topic_name(i);
        let base_offset = *offsets.get(&topic).expect("offsets");

        let validation = validate_consume_for_topic(
            topic.clone(),
            base_offset,
            option.clone(),
            Some(sender.clone()),
        );
        if option.consumer_wait {
            validation.await
        } else {
            async_consumers.insert(topic);
            spawn(validation);
        }
    }

    // wait for all async tests
    let mut timer = sleep(Duration::from_secs(120));
    loop {
        if async_consumers.is_empty() {
            break;
        }

        select! {
            _ = &mut timer => {
                panic!("test timer expired");
            },
            msg = receiver.next() => {
                if let Some(topic) = msg {
                    println!("test for topic: {} completed",topic);
                    async_consumers.remove(&topic);
                } else {
                    panic!("test channel expired, tests not copmleted with: {:#?}",async_consumers);
                }

            }
        }
    }

    println!("all tests copmleted");
}

async fn validate_consume_for_topic(
    topic: String,
    base_offset: i64,
    option: TestOption,
    ack: Option<Sender<String>>,
) {
    println!(
        "starting consumer validation for:  {} base_offset: {}, iterations: {}",
        topic, base_offset, option.produce.produce_iteration
    );

    let client = Fluvio::connect().await.expect("should connect");
    let consumer = client
        .partition_consumer(topic.clone(), 0)
        .await
        .expect("expected consumer");

    let mut stream = consumer
        .stream(
            Offset::absolute(base_offset)
                .unwrap_or_else(|_| panic!("creating stream for iteration: {}", topic)),
        )
        .await
        .expect("stream");

    let iteration = option.produce.produce_iteration;
    let mut total_records: u16 = 0;
    while let Some(Ok(record)) = stream.next().await {
        let offset = record.offset();
        if let Some(bytes) = record.try_into_bytes() {
            info!(
                "* consumer iter: {}, received offset: {}, bytes: {}",
                total_records,
                offset,
                bytes.len()
            );
            validate_message(iteration, offset, &topic, &option, &bytes);
            info!(
                " total records: {}, validated offset: {}",
                total_records, offset
            );
            total_records += 1;
            if total_records == iteration {
                println!("<<consume test done for: {} >>>>", topic);
                break;
            }
        }
    }
    println!("consume message validated for: {}", topic);
    if let Some(sender) = ack {
        sender.send(topic).await.expect("sending result back");
    }
}
