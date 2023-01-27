// test consumer

use std::{io, time::Duration};
use std::io::Write;

use tracing::{info};
use futures_lite::stream::StreamExt;

use fluvio_test_util::test_runner::test_driver::{TestDriver};
use fluvio_test_util::test_meta::environment::EnvDetail;
use fluvio::{Offset};
use fluvio_command::CommandExt;
use crate::get_binary;

use super::SmokeTestCase;
use super::message::*;
use super::offsets::{self, Offsets};

/// verify consumers
pub async fn validate_consume_message(test_driver: TestDriver, test_case: &SmokeTestCase) {
    let use_cli = test_case.option.use_cli;

    // Get offsets before starting
    let offsets = offsets::find_offsets(&test_driver, test_case).await;

    if use_cli {
        validate_consume_message_cli(test_case, offsets);
    } else {
        validate_consume_message_api(test_driver, offsets, test_case).await;
    }
}

fn validate_consume_message_cli(test_case: &SmokeTestCase, offsets: Offsets) {
    let replication = test_case.environment.replication;

    for i in 0..replication {
        let topic_name = test_case.environment.base_topic_name();
        let offset = offsets.get(&topic_name).expect("topic offset");
        let mut command = get_binary("fluvio").expect("fluvio not found");
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
        validate_message(i as u32, *offset, test_case, &msg[0..msg.len() - 1]);

        println!("topic: {topic_name}, consume message validated!");
    }
}

pub async fn validate_consume_message_api(
    test_driver: TestDriver,
    offsets: Offsets,
    test_case: &SmokeTestCase,
) {
    use tokio::select;
    use fluvio_future::timer::sleep;
    use std::time::SystemTime;

    use fluvio_controlplane_metadata::partition::PartitionSpec;

    let producer_iteration = test_case.option.producer_iteration;
    let partition = test_case.environment.partition;
    let topic_name = test_case.environment.base_topic_name();
    let base_offset = offsets.get(&topic_name).expect("offsets");

    println!(">> starting consume validation topic: {topic_name}");

    for i in 0..partition {
        println!(
            "starting fetch stream for: {topic_name} base offset: {base_offset}, expected new records: {producer_iteration}"
        );

        let consumer = test_driver.get_consumer(&topic_name, 0).await;

        let mut stream = consumer
            .stream(
                Offset::absolute(*base_offset)
                    .unwrap_or_else(|_| panic!("creating stream for iteration: {i}")),
            )
            .await
            .expect("stream");

        let mut total_records: u32 = 0;

        let mut chunk_time = SystemTime::now();

        loop {
            let _canow = SystemTime::now();
            select! {

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

                        if test_case.option.skip_consumer_validate {
                            info!(
                                "Skipped message validation",
                            );
                        } else {
                            validate_message(producer_iteration, offset, test_case, bytes);
                        };

                        info!(
                            " total records: {}, validated offset: {}",
                            total_records, offset
                        );
                        total_records += 1;

                        //let mut lock = test_driver.write().await;

                        // record latency
                        //let consume_time = now.elapsed().clone().unwrap().as_nanos();
                        //lock.consume_latency_record(consume_time as u64).await;
                        //lock.consume_bytes_record(bytes.len()).await;

                        // debug!("Consume stat updates: {:?} {:?}", lock.consumer_latency_histogram, lock.consumer_bytes);
                        //debug!(consumer_bytes = lock.consumer_bytes, "Consume stat updates");

                        //drop(lock);

                        // for each
                        if total_records % 100 == 0 {
                            let elapsed_chunk_time = chunk_time.elapsed().clone().unwrap().as_secs_f32();
                            println!("total processed records: {total_records} chunk time: {elapsed_chunk_time:.1} secs");
                            info!(total_records,"processed records");
                            chunk_time = SystemTime::now();
                        }

                        if total_records == producer_iteration {
                            println!("consume message validated!, records: {total_records}");
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

    // This check is unreliable when we are testing w/ connectors, so skip it
    if test_case.option.connector_config.is_some() {
        println!("skipping replication status verification");
    } else {
        let replication = test_case.environment.replication;
        if replication > 1 {
            let wait_value = std::env::var("FLV_SHORT_RECONCILLATION").unwrap_or_default();
            let wait_delay_sec: u64 = wait_value.parse().unwrap_or(30);

            println!("waiting {wait_delay_sec} seconds to verify replication status...");
            // wait 5 seconds to get status and ensure replication is done
            sleep(Duration::from_secs(wait_delay_sec)).await;

            let admin = test_driver.client().admin().await;
            let partitions = admin.all::<PartitionSpec>().await.expect("partitions");

            println!("partitions: {partitions:#?}");

            assert_eq!(partitions.len(), 1);

            let test_topic = &partitions[0];
            let status = &test_topic.status;
            let leader = &status.leader;

            assert_eq!(leader.leo, base_offset + producer_iteration as i64);
            assert_eq!(status.replicas.len() as u16, replication - 1);

            for i in 0..replication - 1 {
                let follower_status = &status.replicas[i as usize];
                assert_eq!(follower_status.hw, producer_iteration as i64);
                assert_eq!(follower_status.leo, producer_iteration as i64);
            }
        }
        println!("replication status verified");
    }

    println!("performing 2nd fetch check. waiting 5 seconds");

    // do complete fetch, since producer has completed, we should retrieve everything
    sleep(Duration::from_secs(5)).await;

    for i in 0..partition {
        println!(
            "performing complete  fetch stream for: {topic_name} base offset: {base_offset}, expected new records: {producer_iteration}"
        );

        let consumer = test_driver.get_consumer(&topic_name, 0).await;

        let mut stream = consumer
            .stream(
                Offset::absolute(*base_offset)
                    .unwrap_or_else(|_| panic!("creating stream for iteration: {i}")),
            )
            .await
            .expect("stream");

        let mut total_records: u32 = 0;

        loop {
            select! {

                stream_next = stream.next() => {

                    let record = stream_next.expect("some").expect("records");
                    let offset = record.offset();
                    let bytes = record.value();
                    info!(
                        "2nd fetch full * consumer iter: {}, received offset: {}, bytes: {}",
                        total_records,
                        offset,
                        bytes.len()
                    );

                    if test_case.option.skip_consumer_validate {
                        info!(
                            "Skipped message validation",
                        );
                    } else {
                        validate_message(producer_iteration, offset, test_case, bytes);
                    };


                    info!(
                        "2nd fetch total records: {}, validated offset: {}",
                        total_records, offset
                    );

                    total_records += 1;

                    if total_records == producer_iteration {
                        break;
                    }

                }

            }
        }
    }

    println!("full <<consume test done for: {topic_name} >>>>");
}
