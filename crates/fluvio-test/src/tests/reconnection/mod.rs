use std::time::Duration;

use futures_lite::stream::StreamExt;
use clap::Parser;

use fluvio::{Offset, RecordKey};
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_future::timer::sleep;
use fluvio_test_derive::fluvio_test;
use fluvio_test_case_derive::MyTestCase;
use fluvio_test_util::{test_meta::environment::EnvDetail, test_runner::test_driver::TestDriver};

// time to wait for ac
const ACK_WAIT: u64 = 5;

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq, MyTestCase)]
#[command(name = "Fluvio reconnection Test")]
pub struct ReconnectionTestOption {}

#[fluvio_test(topic = "reconnection", async)]
pub async fn reconnection(mut test_driver: TestDriver, mut test_case: TestCase) {
    reconnect_producer(&test_driver, &test_case).await;

    reconnect_consumer(&test_driver, &test_case).await;
}

async fn reconnect_producer(test_driver: &TestDriver, test_case: &TestCase) {
    println!("Starting reconnection test");

    let topic_name = test_case.environment.base_topic_name();
    let producer = test_driver.create_producer(&topic_name).await;
    println!("sending first record");

    producer
        .send(RecordKey::NULL, "msg1_producer")
        .await
        .expect("sending");

    producer.flush().await.expect("flushing");

    let leader = get_leader(test_driver).await;
    terminate_spu(test_driver, leader).await;
    sleep(Duration::from_secs(ACK_WAIT)).await;
    start_spu(test_driver, leader).await;
    sleep(Duration::from_secs(ACK_WAIT)).await;

    println!("sending second record");
    producer
        .send(RecordKey::NULL, "msg2_producer")
        .await
        .expect("sending");

    producer.flush().await.expect("flushing");

    let mut stream = test_driver
        .get_consumer_with_start(&topic_name, 0, Offset::absolute(0).expect("offset"))
        .await;

    println!("checking msg1");
    let records = stream.next().await.expect("get next").expect("next");
    assert_eq!(records.value(), "msg1_producer".as_bytes());

    println!("checking msg2");
    let records = stream.next().await.expect("get next").expect("next");
    assert_eq!(records.value(), "msg2_producer".as_bytes());
}

async fn reconnect_consumer(test_driver: &TestDriver, test_case: &TestCase) {
    println!("Starting reconnection test");

    let topic_name = test_case.environment.base_topic_name();
    let producer = test_driver.create_producer(&topic_name).await;
    println!("sending first record");

    let mut stream = test_driver
        .get_consumer_with_start(&topic_name, 0, Offset::end())
        .await;

    producer
        .send(RecordKey::NULL, "msg1_consume")
        .await
        .expect("sending");

    producer.flush().await.expect("flushing");

    println!("checking msg1");
    let records = stream.next().await.expect("get next").expect("next");
    assert_eq!(records.value(), "msg1_consume".as_bytes());

    let leader = get_leader(test_driver).await;
    terminate_spu(test_driver, leader).await;
    sleep(Duration::from_secs(ACK_WAIT)).await;
    start_spu(test_driver, leader).await;
    sleep(Duration::from_secs(ACK_WAIT)).await;

    println!("sending second record");
    producer
        .send(RecordKey::NULL, "msg2_consume")
        .await
        .expect("sending");

    producer.flush().await.expect("flushing");

    println!("checking msg2");
    let records = stream.next().await.expect("get next").expect("next");
    assert_eq!(records.value(), "msg2_consume".as_bytes());
}

async fn get_leader(test_driver: &TestDriver) -> i32 {
    let admin = test_driver.client().admin().await;
    let partitions = admin.all::<PartitionSpec>().await.expect("partitions");
    let test_topic = &partitions[0];
    let leader = test_topic.spec.leader;
    println!("spu id is: {leader}");
    leader
}

async fn terminate_spu(test_driver: &TestDriver, leader: i32) {
    let cluster_manager = test_driver
        .get_cluster()
        .expect("cluster")
        .env_driver()
        .create_cluster_manager();
    println!("terminating spu: {}", &leader);
    cluster_manager.terminate_spu(leader).expect("terminate");
}

async fn start_spu(test_driver: &TestDriver, leader: i32) {
    let cluster_manager = test_driver
        .get_cluster()
        .expect("cluster")
        .env_driver()
        .create_cluster_manager();
    println!("starting spu again: {}", &leader);
    let leader_spu = cluster_manager.create_spu_absolute(leader as u16);
    leader_spu.start().expect("start");
}
