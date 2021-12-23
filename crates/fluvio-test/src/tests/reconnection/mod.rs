use std::any::Any;
use std::time::Duration;

use futures_lite::stream::StreamExt;

use fluvio::{Offset, RecordKey};
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_future::timer::sleep;
use structopt::StructOpt;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};

// time to wait for ac
const ACK_WAIT: u64 = 20;

#[derive(Debug, Clone)]
pub struct ReconnectionTestCase {
    pub environment: EnvironmentSetup,
    pub option: ReconnectionTestOption,
}

impl From<TestCase> for ReconnectionTestCase {
    fn from(test_case: TestCase) -> Self {
        let reconnection_option = test_case
            .option
            .as_any()
            .downcast_ref::<ReconnectionTestOption>()
            .expect("ReconnectionTestOption")
            .to_owned();
        Self {
            environment: test_case.environment,
            option: reconnection_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio reconnection Test")]
pub struct ReconnectionTestOption {}

impl TestOption for ReconnectionTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(topic = "reconnection", async)]
pub async fn reconnection(mut test_driver: TestDriver, mut test_case: TestCase) {
    println!("Starting reconnection test");

    // first a create simple message
    let topic_name = test_case.environment.topic_name();
    let producer = test_driver.create_producer(&topic_name).await;
    println!("sending first record");

    producer
        .send(RecordKey::NULL, "msg1")
        .await
        .expect("sending");

    producer.flush().await.expect("flusing");

    let admin = test_driver.client().admin().await;

    let partitions = admin
        .list::<PartitionSpec, _>(vec![])
        .await
        .expect("partitions");

    let test_topic = &partitions[0];
    let leader = test_topic.spec.leader;
    println!("spu id is: {}", leader);

    let cluster_manager = test_driver
        .get_cluster()
        .expect("cluster")
        .env_driver()
        .create_cluster_manager();

    println!("terminating spu: {}", &leader);

    cluster_manager.terminate_spu(leader).expect("terminate");

    sleep(Duration::from_secs(ACK_WAIT)).await;

    println!("starting spu again: {}", &leader);

    let leader_spu = cluster_manager.create_spu_absolute(leader as u16);
    leader_spu.start().expect("start");

    sleep(Duration::from_secs(ACK_WAIT)).await;

    println!("sending second record");
    // Use the same producer
    producer
        .send(RecordKey::NULL, "msg2")
        .await
        .expect("sending");
    producer.flush().await.expect("flusing");

    let consumer = test_driver.get_consumer(&topic_name, 0).await;
    let mut stream = consumer
        .stream(Offset::absolute(0).expect("offset"))
        .await
        .expect("stream");

    println!("checking msg1");
    let records = stream.next().await.expect("get next").expect("next");
    assert_eq!(records.value(), "msg1".as_bytes());

    println!("checking msg2");
    let records = stream.next().await.expect("get next").expect("next");
    assert_eq!(records.value(), "msg2".as_bytes());
}
