// test client

use std::io;
use std::io::Write;

use utils::bin::get_fluvio;

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
    
    let topic_name = &option.topic_name;

    let output = get_fluvio()
        .expect("fluvio not founded")
        .arg("consume")
        .arg(topic_name)
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
    validate_message(0, option,&msg[0..msg.len()-1]);

    println!("consume message validated!");
}


async fn validate_consume_message_api(option: &TestOption) {
    // futures::stream::StreamExt;

    use flv_client::profile::ScConfig;
    use flv_client::SpuController;
    use flv_client::FetchOffset;
    use flv_client::ReplicaLeader;
    use flv_client::FetchLogOption;

    let config = ScConfig::new(None, None).expect("connect");
    let mut sc = config.connect().await.expect("should connect");

    let topic_name = &option.topic_name;
    let mut leader = sc
        .find_replica_for_topic_partition(topic_name, 0)
        .await
        .expect("leader not founded");


    println!("retrieving messages");
    let response = leader.fetch_logs_once(FetchOffset::Earliest(None), FetchLogOption::default()).await.expect("records");
    println!("message received");
    let batches = response.records.batches;

    assert_eq!(batches.len(),option.produce.produce_iteration as usize);
    println!("consume message validated!");
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
