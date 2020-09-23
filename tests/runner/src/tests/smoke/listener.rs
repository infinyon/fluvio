use std::time::Duration;

use futures::stream::StreamExt;
use futures::select;
use futures::future::FutureExt;

use fluvio_future::timer::sleep;
use fluvio::profile::ScConfig;
use fluvio::SpuController;
use fluvio::ReplicaLeader;
use fluvio::FetchLogOption;
use fluvio::FetchOffset;
use fluvio::profile::TlsConfig;
use fluvio::profile::TlsClientConfig;

use crate::TestOption;
use crate::tls::Cert;

#[allow(unused)]
/// test when consuming using streaming mode
pub async fn validate_consumer_listener(client_idx: u16, option: &TestOption) {
    println!(
        "starting consumer validation: {}, sleeping 1 sec",
        client_idx
    );

    let client_cert = Cert::load_client();

    let tls_option = if option.tls() {
        Some(TlsConfig::File(TlsClientConfig {
            client_cert: client_cert.cert.display().to_string(),
            client_key: client_cert.key.display().to_string(),
            ca_cert: client_cert.ca.display().to_string(),
            domain: "fluvio.local".to_owned(),
        }))
    } else {
        None
    };

    let config = ScConfig::new(Some("localhost:9003".into()), tls_option).expect("connect");
    let mut sc = config.connect().await.expect("should connect");

    let mut leader = sc
        .find_replica_for_topic_partition("test1", 0)
        .await
        .expect("leader not founded");

    let fetch_option = FetchLogOption::default();

    let mut log_stream = leader
        .fetch_logs(FetchOffset::Offset(-1), fetch_option)
        .fuse();

    println!("got log stream, testing {}", client_idx);

    let first_response = log_stream.next().await.expect("response");
    let records = first_response.records;
    // no records since we don't have any produce
    assert_eq!(records.batches.len(), 0, "there should not be any records");

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
        _ = (sleep(Duration::from_secs(3))).fuse() => {
            assert!(false,"consumer: {} didn't receive any",client_idx)
        }

        complete => {},
    }

    println!("consumer listener test success: {}", client_idx);
}
