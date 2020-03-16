use std::time::Duration;
use flv_future_aio::timer::sleep;
use flv_client::profile::ScConfig;
use flv_client::SpuController;
use flv_client::ReplicaLeader;


/// produce message
pub async fn produce_message() {

    sleep(Duration::from_secs(2)).await;

    let config = ScConfig::new(Some("localhost:9003".into()), None).expect("connect");
    let mut sc = config.connect().await.expect("should connect");

    let mut leader = sc.find_replica_for_topic_partition("test1",0).await.expect("leader not founded");

    let message = "hello world".to_owned().into_bytes();

    leader.send_record(message).await.expect("message sent");
}
