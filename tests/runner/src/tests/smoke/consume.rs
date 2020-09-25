// test consumer

use std::io;
use std::io::Write;
use std::collections::HashMap;

use utils::bin::get_fluvio;

use fluvio::{Fluvio, Offset};
use crate::cli::TestOption;
use crate::util::CommandUtil;
use super::message::*;

type Offsets = HashMap<String,i64>;

/// verify consumers
pub async fn validate_consume_message(option: &TestOption,offsets: Offsets) {
    if option.produce.produce_iteration == 1 {
        validate_consume_message_cli(option,offsets);
    } else {
        validate_consume_message_api(option).await;
    }
}

fn validate_consume_message_cli(option: &TestOption,offsets: Offsets) {
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
        validate_message(*offset, &topic_name,option, &msg[0..msg.len() - 1]);

        println!("topic: {}, consume message validated!", topic_name);
    }
}

async fn validate_consume_message_api(option: &TestOption) {
    let client = Fluvio::connect().await.expect("should connect");
    let replication = option.replication();

    for i in 0..replication {
        let topic_name = option.topic_name(i);
        let consumer = client
            .partition_consumer(topic_name, 0)
            .await
            .expect("consumer");

        let mut stream = consumer.stream(Offset::beginning()).await.expect("start from beginning");

        while let Ok(event) = stream.next().await {
            for batch in event.partition.records.batches {
                for record in batch.records {
                    if let Some(record) = record.value.inner_value() {
                        let string = String::from_utf8(record).unwrap();
                        
                    }
                }
            }
        }

        println!("retrieving messages");
        let response = consumer.fetch(Offset::beginning()).await.expect("records");
        println!("message received");
        let batches = response.records.batches;

        assert_eq!(batches.len(), option.produce.produce_iteration as usize);
        println!("consume message validated!");
    }

    /*
    let mut log_stream = leader.fetch_logs(FetchOffset::Earliest(None), FetchLogOption::default());

    if let Some(partition_response) = log_stream.next().await {
        let records = partition_response.records;

        println!("batch records: {}", records.batches.len());
    } else {
        assert!(false, "no response")
    }
    */
}
