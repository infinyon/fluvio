// test consumer

use std::io;
use std::io::Write;
use std::collections::HashMap;

use utils::bin::get_fluvio;
use futures::StreamExt;

use fluvio::{Fluvio, Offset};
use crate::cli::TestOption;
use crate::util::CommandUtil;
use super::message::*;

type Offsets = HashMap<String, i64>;

/// verify consumers
pub async fn validate_consume_message(option: &TestOption, offsets: Offsets) {
    if option.produce.produce_iteration == 1 {
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
        validate_message(*offset, &topic_name, option, &msg[0..msg.len() - 1]);

        println!("topic: {}, consume message validated!", topic_name);
    }
}

async fn validate_consume_message_api(offsets: Offsets, option: &TestOption) {
    let client = Fluvio::connect().await.expect("should connect");
    let replication = option.replication();
    let iteration = option.produce.produce_iteration;

    for i in 0..replication {
        let topic_name = option.topic_name(i);
        let base_offset = offsets.get(&topic_name).expect("offsets");
        println!(
            "starting fetch stream for: {} at: {}, expected new records: {}",
            topic_name, base_offset, iteration
        );

        let consumer = client
            .partition_consumer(topic_name.clone(), 0)
            .await
            .expect("consumer");

        let mut stream = consumer
            .stream(Offset::absolute(*base_offset).unwrap())
            .await
            .expect("start from beginning");

        let mut total_records: u16 = 0;
        while let Some(Ok(record)) = stream.next().await {
            let offset = record.offset();
            if let Some(bytes) = record.try_into_bytes() {
                validate_message(offset, &topic_name, option, &bytes);
                println!(
                    "   consumer: total records: {}, validated offset: {}",
                    total_records, offset
                );
                total_records += 1;
                assert_eq!(offset, base_offset + total_records as i64);
                if total_records == iteration {
                    println!("<<consume test done for: {} >>>>", topic_name);
                    break;
                }
            }
        }
        println!("consume message validated!");
    }
}
