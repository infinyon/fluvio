// test client

use std::io;
use std::io::Write;

use utils::bin::get_fluvio;

use crate::cli::TestOption;
use crate::util::CommandUtil;

/// verify consumer thru CLI
pub async fn validate_consume_message(option: &TestOption) {

    if option.produce.produce_count == 1 {
        validate_consume_message_cli(option);
    } else {
       // validate_consume_message_api(option).await;
    }

}


fn validate_consume_message_cli(option: &TestOption) {

    use super::MESSAGE_PREFIX;


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

    let msg = format!("message: {}, {}\n",0,MESSAGE_PREFIX);

    assert_eq!(output.stdout.as_slice(),msg.as_bytes());

    println!("consume message validated!");

}

#[allow(unused)]
async fn validate_consume_message_api(option: &TestOption) {

    use futures::stream::StreamExt;

    use flv_client::profile::ScConfig;
    use flv_client::SpuController;
    use flv_client::FetchOffset;
    use flv_client::ReplicaLeader;
    use flv_client::FetchLogOption;

    let config = ScConfig::new(None,None).expect("connect");
    let mut sc = config.connect().await.expect("should connect");

    
    let topic_name = &option.topic_name;
    let mut leader = sc.find_replica_for_topic_partition(topic_name,0).await.expect("leader not founded");

    let mut log_stream = leader.fetch_logs(FetchOffset::Earliest(None),FetchLogOption::default());

    
    if let Some(partition_response) =  log_stream.next().await {
        let records = partition_response.records;

        println!("batch records: {}",records.batches.len());
    } else {
        assert!(false,"no response")
    }

   

}