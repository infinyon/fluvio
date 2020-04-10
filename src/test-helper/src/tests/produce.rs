use std::time::Duration;
use flv_future_aio::timer::sleep;
use flv_client::profile::ScConfig;
use flv_client::SpuController;
use flv_client::ReplicaLeader;


/// produce message
#[allow(unused)]
pub async fn produce_message_with_api() {

    sleep(Duration::from_secs(2)).await;

    let config = ScConfig::new(Some("localhost:9003".into())).expect("connect");
    let mut sc = config.connect().await.expect("should connect");

    let mut leader = sc.find_replica_for_topic_partition("test1",0).await.expect("leader not founded");

    let message = "hello world".to_owned().into_bytes();

    leader.send_record(message).await.expect("message sent");
}

use std::io::Write;
use std::process::Stdio;

use crate::command_spawn;


pub fn produce_message_with_cli() {

    let mut child = command_spawn("fluvio","produce",
            | cmd | {

                cmd
                    .stdin(Stdio::piped())
                    .arg("produce")
                    .arg("--topic")
                    .arg("test1")
                    .arg("--sc")
                    .arg("localhost:9003");

                println!("produce cmd: {:#?}",cmd);

            });

    let stdin = child.stdin.as_mut().expect("Failed to open stdin");
    stdin.write_all("hello, world".as_bytes()).expect("Failed to write to stdin");

    let status = child.wait().expect("Failed to read stdout");

    assert!(status.success());

    println!("produce message: hello world");

}