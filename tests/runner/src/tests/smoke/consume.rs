// test client

use std::io;
use std::io::Write;

use utils::bin::get_fluvio;

use fluvio::{Fluvio, Offset};
use crate::cli::TestOption;
use crate::util::CommandUtil;
use super::message::*;

/// verify consumer thru CLI
pub async fn validate_consume_message(option: &TestOption) {
    if option.produce.produce_iteration == 1 {
        validate_consume_message_cli(option);
    } else {
        validate_consume_message_api(option).await;
    }
}

fn validate_consume_message_cli(option: &TestOption) {
    let replication = option.replication();

    for i in 0..replication {
        let topic_name = option.topic_name(i);
        let output = get_fluvio()
            .expect("fluvio not founded")
            .arg("consume")
            .arg(&topic_name)
            .arg("--partition")
            .arg("0")
            .arg("-B")
            .arg("-d")
            .print()
            .output()
            .expect("no output");

        // io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();

        let msg = output.stdout.as_slice();
        validate_message(0, option, &msg[0..msg.len() - 1]);

        println!("topic: {}, consume message validated!", topic_name);
    }
}

async fn validate_consume_message_api(option: &TestOption) {
    use fluvio::params::FetchLogOption;

    let client = Fluvio::connect().await.expect("should connect");
    let replication = option.replication();

    for i in 0..replication {
        let topic_name = option.topic_name(i);
        let consumer = client
            .partition_consumer(topic_name, 0)
            .await
            .expect("consumer");

        println!("retrieving messages");
        let response = consumer
            .fetch(Offset::from_beginning(0).unwrap(), FetchLogOption::default())
            .await
            .expect("records");
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
