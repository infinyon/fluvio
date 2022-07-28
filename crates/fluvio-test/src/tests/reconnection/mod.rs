use std::any::Any;
use std::time::Duration;

use futures_lite::stream::StreamExt;

use fluvio::{Offset, RecordKey};
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_future::timer::sleep;
use clap::Parser;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use tracing::{Instrument, debug_span, trace_span};

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

#[derive(Debug, Clone, Parser, Default, PartialEq)]
#[clap(name = "Fluvio reconnection Test")]
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
    let topic_name = test_case.environment.base_topic_name();
    let producer = test_driver
        .create_producer(&topic_name)
        .instrument(debug_span!("producer_create"))
        .await;
    println!("sending first record");

    producer
        .send(RecordKey::NULL, "msg1")
        .instrument(debug_span!("producer_send_1"))
        .await
        .expect("sending");

    producer
        .flush()
        .instrument(debug_span!("producer_flush_1"))
        .await
        .expect("flushing");

    let admin = test_driver
        .client()
        .admin()
        .instrument(trace_span!("fluvio_admin"))
        .await;

    let partitions = admin
        .list::<PartitionSpec, _>(vec![])
        .instrument(trace_span!("list_partitions"))
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

    sleep(Duration::from_secs(ACK_WAIT))
        .instrument(trace_span!("sleep"))
        .await;

    println!("starting spu again: {}", &leader);

    let leader_spu = cluster_manager.create_spu_absolute(leader as u16);
    leader_spu.start().expect("start");

    sleep(Duration::from_secs(ACK_WAIT))
        .instrument(trace_span!("sleep"))
        .await;

    producer
        .clear_errors()
        .instrument(debug_span!("producer_clear_errors"))
        .await;

    println!("sending second record");
    // Use the same producer
    producer
        .send(RecordKey::NULL, "msg2")
        .instrument(debug_span!("producer_send_2"))
        .await
        .expect("sending");

    producer
        .flush()
        .instrument(debug_span!("producer_flush_2"))
        .await
        .expect("flushing");

    let consumer = test_driver
        .get_consumer(&topic_name, 0)
        .instrument(debug_span!("consumer_create"))
        .await;
    let mut stream = consumer
        .stream(Offset::absolute(0).expect("offset"))
        .instrument(debug_span!("stream_create"))
        .await
        .expect("stream");

    println!("checking msg1");
    let records = stream
        .next()
        .instrument(debug_span!("stream_next_1"))
        .await
        .expect("get next")
        .expect("next");
    assert_eq!(records.value(), "msg1".as_bytes());

    println!("checking msg2");
    let records = stream
        .next()
        .instrument(debug_span!("stream_next_2"))
        .await
        .expect("get next")
        .expect("next");
    assert_eq!(records.value(), "msg2".as_bytes());
}
