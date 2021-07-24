use std::sync::Arc;
use std::collections::HashMap;
use std::time::SystemTime;

use fluvio_test_util::test_runner::test_driver::{
    TestDriver, TestDriverType, TestProducer, PulsarTestData,
};
use tracing::{info, debug};

use super::SmokeTestCase;
use super::message::*;
use fluvio::RecordKey;
use fluvio_command::CommandExt;
use async_lock::RwLock;

use csv::WriterBuilder;

type Offsets = HashMap<String, i64>;

pub async fn produce_message(
    test_driver: Arc<RwLock<TestDriver>>,
    test_case: &SmokeTestCase,
) -> Offsets {
    use fluvio_future::task::spawn;
    let lock = test_driver.read().await;
    // get initial offsets for each of the topic
    let offsets = offsets::find_offsets(&*lock, &test_case).await;
    drop(lock);

    let use_cli = test_case.option.use_cli;
    let consumer_wait = test_case.option.consumer_wait;

    if use_cli {
        cli::produce_message_with_cli(test_case, offsets.clone()).await;
    } else if consumer_wait {
        produce_message_with_api(test_driver, offsets.clone(), test_case.clone()).await;
    } else {
        // TODO: Support multiple producer

        for _p in 0..test_case.environment.producers {
            spawn(produce_message_with_api(
                test_driver.clone(),
                offsets.clone(),
                test_case.clone(),
            ));
        }
    }

    offsets
}

mod offsets {

    use std::collections::HashMap;

    use fluvio::FluvioAdmin;

    use fluvio_controlplane_metadata::partition::PartitionSpec;
    use fluvio_controlplane_metadata::partition::ReplicaKey;

    use super::SmokeTestCase;
    use super::{TestDriver, TestDriverType};

    pub async fn find_offsets(
        test_driver: &TestDriver,
        test_case: &SmokeTestCase,
    ) -> HashMap<String, i64> {
        let partition = test_case.environment.partition;

        let _consumer_wait = test_case.option.consumer_wait;

        let mut offsets = HashMap::new();

        match test_driver.client.as_ref() {
            TestDriverType::Fluvio(fluvio_client) => {
                let client = fluvio_client.clone();
                let mut admin = client.admin().await;

                // TODO: Support partition
                for _i in 0..partition {
                    let topic_name = test_case.environment.topic_name.clone();
                    // find last offset
                    let offset = last_leo(&mut admin, &topic_name).await;
                    println!("found topic: {} offset: {}", topic_name, offset);
                    offsets.insert(topic_name, offset);
                }
            }
            _ => {
                // Do we need to support getting partition offset maps from other clusters?
                ()
            }
        }

        offsets
    }

    async fn last_leo(admin: &mut FluvioAdmin, topic: &str) -> i64 {
        use std::convert::TryInto;

        let partitions = admin
            .list::<PartitionSpec, _>(vec![])
            .await
            .expect("get partitions status");

        for partition in partitions {
            let replica: ReplicaKey = partition
                .name
                .clone()
                .try_into()
                .expect("canot parse partition");

            if replica.topic == topic && replica.partition == 0 {
                return partition.status.leader.leo;
            }
        }

        panic!("cannot found partition 0 for topic: {}", topic);
    }
}

pub async fn produce_message_with_api(
    test_driver: Arc<RwLock<TestDriver>>,
    offsets: Offsets,
    test_case: SmokeTestCase,
) {
    use std::time::Duration;
    use fluvio_future::timer::sleep;

    let produce_iteration = test_case.option.producer_iteration;
    let topic_name = test_case.environment.topic_name.clone();
    let lock = test_driver.read().await;

    // TODO: Deal with offsets for other clusters
    let base_offset = if let TestDriverType::Fluvio(_) = lock.client.as_ref() {
        *offsets.get(&topic_name).expect("offsets")
    } else {
        0
    };

    let batch_size_bytes = test_case.environment.batch_kbytes * 1000;
    let batch_time = test_case.environment.batch_ms;

    drop(lock);

    let mut lock = test_driver.write().await;
    let mut producer = lock.get_producer(&topic_name).await;
    drop(lock);

    let mut buffer_data_count: usize = 0;
    let mut producer_buffer = Vec::new();
    let mut batch_timer = SystemTime::now();

    for i in 0..produce_iteration {
        let offset = base_offset + i as i64;

        let mut wtr = WriterBuilder::new().has_headers(false).from_writer(vec![]);
        let gen_msg = TestMessage::generate_message(offset, &test_case);
        wtr.serialize(gen_msg)
            .expect("TestMessage serialize to csv failed");
        let message = wtr.into_inner().expect("csv to Vec<u8> failed");

        match producer {
            // Batch accumulation is handled in memory
            // Then when filesize or timer triggers, send buffer
            TestProducer::Fluvio(ref p) => {
                let len = message.len();
                info!("trying send: {}, iteration: {}", topic_name, i);

                producer_buffer.push((RecordKey::NULL, message.clone()));
                buffer_data_count += message.len();

                let is_batch_time_met = batch_timer
                    .elapsed()
                    .expect("Unable to get batch time elapsed")
                    >= Duration::from_millis(batch_time);
                let is_buffer_size_met = buffer_data_count >= batch_size_bytes;
                let is_last_iteration = i == produce_iteration - 1;

                if is_batch_time_met || is_buffer_size_met || is_last_iteration {
                    debug!(
                        "batch time trigger: {:#} buffer size trigger:{:#} last?: {:#}",
                        is_batch_time_met, is_buffer_size_met, is_last_iteration
                    );

                    let mut lock = test_driver.write().await;
                    lock.fluvio_send(&p, producer_buffer)
                        .await
                        .unwrap_or_else(|_| panic!("send record failed for iteration: {}", i));
                    drop(lock);
                    producer_buffer = Vec::new();
                    buffer_data_count = 0;
                    batch_timer = SystemTime::now();
                }

                info!(
                    "completed send iter: {}, offset: {},len: {}",
                    topic_name, offset, len
                );
                sleep(Duration::from_millis(10)).await;
            }
            // Pulsar producer should have been configured for batch w/r/t # of messages
            // TODO: We need to keep track of timeout, and manually flush buffer
            TestProducer::Pulsar(ref mut p) => {
                let mut lock = test_driver.write().await;
                lock.pulsar_send(p, vec![PulsarTestData { data: message }])
                    .await
                    .unwrap_or_else(|_| panic!("send record failed for iteration: {}", i));
                drop(lock);
            }
            // Kafka producer was configured for batching via flv-test cli options
            TestProducer::Kafka(ref mut p) => {
                let mut lock = test_driver.write().await;
                lock.kafka_send(p, topic_name.as_str(), message)
                    .await
                    .unwrap_or_else(|_| panic!("send record failed for iteration: {}", i));
                drop(lock);
            }
        }
    }
}

mod cli {
    use super::*;

    use std::io::Write;
    use std::process::Stdio;
    use fluvio_system_util::bin::get_fluvio;
    use fluvio_future::timer::sleep;
    use std::time::Duration;

    pub async fn produce_message_with_cli(test_case: &SmokeTestCase, offsets: Offsets) {
        println!("starting produce");

        let producer_iteration = test_case.option.producer_iteration;

        for i in 0..producer_iteration {
            produce_message_replication(i, &offsets, test_case);
            sleep(Duration::from_millis(10)).await
        }
    }

    fn produce_message_replication(iteration: u16, offsets: &Offsets, test_case: &SmokeTestCase) {
        let replication = test_case.environment.replication;

        for _i in 0..replication {
            produce_message_inner(iteration, offsets, test_case);
        }
    }

    fn produce_message_inner(_iteration: u16, offsets: &Offsets, test_case: &SmokeTestCase) {
        use std::io;
        let topic_name = test_case.environment.topic_name.as_str();

        let base_offset = *offsets.get(topic_name).expect("offsets");

        info!(
            "produce cli message: {}, offset: {}",
            topic_name, base_offset
        );

        let mut child = get_fluvio().expect("no fluvio");
        if let Some(log) = &test_case.environment.client_log {
            child.env("RUST_LOG", log);
        }
        child.stdin(Stdio::piped()).arg("produce").arg(topic_name);
        println!("Executing: {}", child.display());
        let mut child = child.spawn().expect("no child");

        let stdin = child.stdin.as_mut().expect("Failed to open stdin");

        let mut wtr = WriterBuilder::new().has_headers(false).from_writer(vec![]);
        let gen_msg = TestMessage::generate_message(base_offset, &test_case);
        wtr.serialize(gen_msg)
            .expect("TestMessage serialize to csv failed");
        let msg = wtr.into_inner().expect("csv to Vec<u8> failed");

        stdin
            .write_all(msg.as_slice())
            .expect("Failed to write to stdin");

        let output = child.wait_with_output().expect("Failed to read stdout");
        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();
        assert!(output.status.success());

        println!("send message of len {}", msg.len());
    }
}
