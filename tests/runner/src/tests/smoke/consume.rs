// test consumer

use std::{io, time::Duration};
use std::io::Write;
use std::collections::HashMap;
use std::sync::Arc;
use async_lock::RwLock;

use tracing::{info, debug};
use futures_lite::stream::StreamExt;

use fluvio_system_util::bin::get_fluvio;
use fluvio_test_util::test_runner::FluvioTestDriver;
use fluvio::Offset;
use fluvio_command::CommandExt;

use super::SmokeTestCase;
use super::message::*;

type Offsets = HashMap<String, i64>;

/// total time allotted for consumer test
fn consume_wait_timeout() -> u64 {
    let var_value = std::env::var("FLV_TEST_CONSUMER_WAIT").unwrap_or_default();
    var_value.parse().unwrap_or(30000) // 30 seconds
}

/// verify consumers
pub async fn validate_consume_message(
    test_driver: Arc<RwLock<FluvioTestDriver>>,
    test_case: &SmokeTestCase,
    offsets: Offsets,
) {
    let use_cli = test_case.option.use_cli;

    if use_cli {
        validate_consume_message_cli(test_case, offsets);
    } else {
        validate_consume_message_api(test_driver, offsets, test_case).await;
    }
}

fn validate_consume_message_cli(test_case: &SmokeTestCase, offsets: Offsets) {
    let replication = test_case.environment.replication;

    for i in 0..replication {
        let topic_name = test_case.environment.topic_name.clone();
        let offset = offsets.get(&topic_name).expect("topic offset");
        let mut command = get_fluvio().expect("fluvio not found");
        command
            .arg("consume")
            .arg(&topic_name)
            .arg("--partition")
            .arg("0")
            .arg("-d")
            .arg("-o")
            .arg(offset.to_string());
        println!("Executing> {}", command.display());
        let output = command.result().expect("fluvio command failed");

        io::stderr().write_all(&output.stderr).unwrap();

        let msg = output.stdout.as_slice();
        validate_message(i, *offset, test_case, &msg[0..msg.len() - 1]);

        println!("topic: {}, consume message validated!", topic_name);
    }
}

async fn validate_consume_message_api(
    test_driver: Arc<RwLock<FluvioTestDriver>>,
    offsets: Offsets,
    test_case: &SmokeTestCase,
) {
    use tokio::select;
    use fluvio_future::timer::sleep;
    use std::time::SystemTime;

    use fluvio_controlplane_metadata::partition::PartitionSpec;

    let producer_iteration = test_case.option.producer_iteration;
    let partition = test_case.environment.partition;
    let topic_name = test_case.environment.topic_name.clone();
    let base_offset = offsets.get(&topic_name).expect("offsets");

    for i in 0..partition {
        println!(
            "starting fetch stream for: {} base offset: {}, expected new records: {}",
            topic_name, base_offset, producer_iteration
        );

        let mut lock = test_driver.write().await;

        let consumer = lock.get_consumer(&topic_name).await;
        drop(lock);

        let mut stream = consumer
            .stream(
                Offset::absolute(*base_offset)
                    .unwrap_or_else(|_| panic!("creating stream for iteration: {}", i)),
            )
            .await
            .expect("stream");

        let mut total_records: u16 = 0;

        // add 30 seconds per 1000
        let cycle = producer_iteration / 1000 + 1;
        let timer_wait = cycle as u64 * consume_wait_timeout();
        info!("total cycle: {}, timer wait: {}", cycle, timer_wait);
        let mut timer = sleep(Duration::from_millis(timer_wait));

        loop {
            let now = SystemTime::now();
            select! {

                _ = &mut timer => {
                    println!("Timeout in timer");
                    debug!("timer expired");
                    panic!("timer expired");
                },

                // max time for each read
                _ = sleep(Duration::from_millis(5000)) => {
                    println!("Timeout in read");
                    panic!("no consumer read iter: current {}",producer_iteration);
                },

                stream_next = stream.next() => {

                    if let Some(Ok(record)) = stream_next {

                        let offset = record.offset();
                        let bytes = record.value();
                        info!(
                            "* consumer iter: {}, received offset: {}, bytes: {}",
                            total_records,
                            offset,
                            bytes.len()
                        );
                        validate_message(producer_iteration, offset, test_case, &bytes);
                        info!(
                            " total records: {}, validated offset: {}",
                            total_records, offset
                        );
                        total_records += 1;

                        let mut lock = test_driver.write().await;

                        // record latency
                        let consume_time = now.elapsed().clone().unwrap().as_nanos();
                        lock.consume_latency_record(consume_time as u64).await;
                        lock.consume_bytes_record(bytes.len()).await;

                        debug!("Consume stat updates: {:?} {:?}", lock.consume_latency, lock.bytes_consumed);

                        drop(lock);


                        if total_records == producer_iteration {
                            println!("<<consume test done for: {} >>>>", topic_name);
                            println!("consume message validated!, records: {}",total_records);
                            break;
                        }

                    } else {
                        println!("I panicked bc of no stream");
                        panic!("no more stream");
                    }
                }

            }
        }
    }

    // wait 15 seconds to get status and ensure replication is done
    sleep(Duration::from_secs(15)).await;

    let lock = test_driver.write().await;
    let admin = lock.client.admin().await;
    let partitions = admin
        .list::<PartitionSpec, _>(vec![])
        .await
        .expect("partitions");
    drop(lock);

    assert_eq!(partitions.len(), 1);

    let test_topic = &partitions[0];
    let status = &test_topic.status;
    let leader = &status.leader;

    assert_eq!(leader.leo, base_offset + producer_iteration as i64);
    println!("status: {:#?}", status);

    let replication = test_case.environment.replication;
    if replication > 1 {
        assert_eq!(status.replicas.len() as u16, replication - 1);

        for i in 0..replication - 1 {
            let follower_status = &status.replicas[i as usize];
            assert_eq!(follower_status.hw, producer_iteration as i64);
            assert_eq!(follower_status.leo, producer_iteration as i64);
        }
    }
}
