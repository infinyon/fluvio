// test client

use std::time::Duration;

use futures::stream::StreamExt;
use futures::select;
use futures::future::FutureExt;

use flv_future_aio::timer::sleep;
use flv_client::profile::ScConfig;
use flv_client::SpuController;
use flv_client::ReplicaLeader;
use flv_client::FetchLogOption;
use flv_client::FetchOffset;

/// test consume message
pub async fn validate_consume_message(client_idx: u16) {

    // give sc, spu time to spin up
    sleep(Duration::from_secs(1)).await;

    println!("starting consumer validation: {}, sleeping 1 sec",client_idx);

    let config = ScConfig::new(Some("localhost:9003".into()), None).expect("connect");
    let mut sc = config.connect().await.expect("should connect");

    let mut leader = sc.find_replica_for_topic_partition("test1",0).await.expect("leader not founded");

    let fetch_option = FetchLogOption::default();

    let mut log_stream = leader.fetch_logs(FetchOffset::Offset(-1), fetch_option).fuse();

    println!("got log stream, testing {}",client_idx);

    let first_response = log_stream.next().await.expect("response");
    let records = first_response.records;
    // no records since we don't have any produce
    assert_eq!(records.batches.len(),0,"there should not be any records");


    select! {
        second_response = log_stream.next() => {
            match second_response {
                None => {
                    assert!(false,"premature termination");
                },
                Some(response) => {
                    let records = response.records;
                    assert_eq!(records.batches.len(),1);
                },
            }
        },
        _ = (sleep(Duration::from_secs(10))).fuse() => {
            assert!(false,"consumer: {} didn't receive any",client_idx)
        }

        complete => {},
    }

    println!("consumer validation done: {}",client_idx);
}
