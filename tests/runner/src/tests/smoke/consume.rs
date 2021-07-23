// test consumer

use std::{io, time::Duration};
use std::io::Write;
use std::collections::HashMap;
use std::sync::Arc;
use async_lock::RwLock;

use tracing::{info, debug, error};
use futures_lite::stream::StreamExt;

use fluvio_system_util::bin::get_fluvio;
use fluvio_test_util::test_runner::test_driver::{FluvioTestDriver, TestConsumer, TestDriverType};
use fluvio::Offset;
use fluvio_command::CommandExt;

use rdkafka::message::Message;

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
    use fluvio_future::task::spawn;
    let use_cli = test_case.option.use_cli;

    if use_cli {
        validate_consume_message_cli(test_case, offsets);
    } else {
        for _c in 0..test_case.environment.consumers {
            spawn(validate_consume_message_api(
                test_driver.clone(),
                offsets.clone(),
                test_case.clone(),
            ))
            .await;
        }
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
        let _valid_msg =
            TestMessage::validate_message(i, *offset, test_case, &msg[0..msg.len() - 1])
                .expect("Message validation failed");

        println!("topic: {}, consume message validated!", topic_name);
    }
}

async fn validate_consume_message_api(
    test_driver: Arc<RwLock<FluvioTestDriver>>,
    offsets: Offsets,
    test_case: SmokeTestCase,
) {
    use tokio::select;
    use fluvio_future::timer::sleep;
    use std::time::SystemTime;

    use fluvio_controlplane_metadata::partition::PartitionSpec;

    let producer_iteration = test_case.option.producer_iteration;
    let partition = test_case.environment.partition;
    let topic_name = test_case.environment.topic_name.clone();

    let lock = test_driver.read().await;
    // TODO: Deal with offsets
    let base_offset = if let TestDriverType::Fluvio(_) = lock.client.as_ref() {
        *offsets.get(&topic_name).expect("offsets")
    } else {
        0
    };

    drop(lock);

    for i in 0..partition {
        println!(
            "starting fetch stream for: {} base offset: {}, expected new records: {}",
            topic_name, base_offset, producer_iteration
        );

        let mut lock = test_driver.write().await;

        let mut consumer = lock.get_consumer(&topic_name, i as i32).await;
        drop(lock);

        match consumer {
            TestConsumer::Fluvio(fluvio_consumer) => {
                let mut stream = fluvio_consumer
                    .stream(
                        Offset::absolute(base_offset)
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
                    let consume_timestamp = SystemTime::now();
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


                                // Stop E2E timer
                                let e2e_stop_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();

                                // Parse record
                                let valid_msg = TestMessage::validate_message(producer_iteration, offset, &test_case, &bytes).expect("Validation failed");

                                // Calculate the E2E duration
                                let e2e_duration_nanos = e2e_stop_time - valid_msg.timestamp;

                                debug!("stop_time: {:#?} duration: {:#?}",e2e_stop_time, e2e_duration_nanos );


                                let mut lock = test_driver.write().await;
                                // record consumer-only and E2E latency
                                let consume_time = consume_timestamp.elapsed().clone().unwrap().as_nanos();
                                lock.consume_record(bytes.len(), consume_time as u64, e2e_duration_nanos as u64).await;
                                drop(lock);



                                info!(
                                    " total records: {}, validated offset: {}",
                                    total_records, offset
                                );
                                total_records += 1;
                                //let lock = test_driver.read().await;
                                ////debug!("Consume stat updates: {:?} {:?}", lock.consume_latency, lock.bytes_consumed);
                                //drop(lock);


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
            TestConsumer::Pulsar(mut pulsar_consumer) => {
                let mut total_records: u16 = 0;

                loop {
                    let consume_timestamp = SystemTime::now();
                    select! {
                        Ok(next) = pulsar_consumer.try_next() => {

                            if let Some(msg) = next {

                                pulsar_consumer.ack(&msg).await.expect("Pulsar consume ask failed");

                                // Stop E2E timer
                                let e2e_stop_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();


                                let data = match msg.deserialize() {
                                    Ok(data) => data.data,
                                    Err(e) => {
                                        error!("could not deserialize message: {:?}", e);
                                        break;
                                    }
                                };

                                // Parse record
                                let offset = 0;
                                let valid_msg = TestMessage::validate_message(producer_iteration, offset, &test_case, &data).expect("Validation failed");

                                // Calculate the E2E duration
                                let e2e_duration_nanos = e2e_stop_time - valid_msg.timestamp;

                                debug!("stop_time: {:#?} duration: {:#?}",e2e_stop_time, e2e_duration_nanos );

                                let mut lock = test_driver.write().await;
                                // record consumer-only and E2E latency
                                let consume_time = consume_timestamp.elapsed().clone().unwrap().as_nanos();

                                lock.consume_record(data.len(), consume_time as u64, e2e_duration_nanos as u64).await;

                                drop(lock);

                                total_records += 1;

                                if total_records == producer_iteration {
                                    println!("<<consume test done for: {} >>>>", topic_name);
                                    println!("consume message validated!, records: {}",total_records);
                                    break;
                                }

                            } else {
                                info!("No more messages from Pulsar");
                                break;
                            }
                        }

                    }
                }
            }
            TestConsumer::Kafka(kafka_consumer) => {
                let mut total_records: u16 = 0;

                let mut stream_msg = kafka_consumer.stream();

                loop {
                    let consume_timestamp = SystemTime::now();
                    select! {
                        Some(next) = stream_msg.next() => {

                            if let Ok(msg) = next {

                                // Stop E2E timer
                                let e2e_stop_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();


                                let data = match msg.payload() {
                                    Some(d) => {
                                        d.to_vec()
                                    },
                                    None => {
                                        panic!("No data in message")
                                    }
                                };

                                // Parse record
                                let offset = 0;
                                let valid_msg = TestMessage::validate_message(producer_iteration, offset, &test_case, &data).expect("Validation failed");

                                // Calculate the E2E duration
                                let e2e_duration_nanos = e2e_stop_time - valid_msg.timestamp;

                                debug!("stop_time: {:#?} duration: {:#?}",e2e_stop_time, e2e_duration_nanos );

                                let mut lock = test_driver.write().await;
                                // record consumer-only and E2E latency
                                let consume_time = consume_timestamp.elapsed().clone().unwrap().as_nanos();

                                lock.consume_record(data.len(), consume_time as u64, e2e_duration_nanos as u64).await;

                                drop(lock);

                                total_records += 1;

                                if total_records == producer_iteration {
                                    println!("<<consume test done for: {} >>>>", topic_name);
                                    println!("consume message validated!, records: {}",total_records);
                                    break;
                                }

                            } else {
                                info!("No more messages from Kafka");
                                break;
                            }
                        }

                    }
                }
            }
        }
    }

    // wait 15 seconds to get status and ensure replication is done
    sleep(Duration::from_secs(15)).await;

    let lock = test_driver.write().await;

    if let TestDriverType::Fluvio(fluvio_client) = lock.client.as_ref() {
        let admin = fluvio_client.admin().await;
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
        //assert_eq!(status.replicas.len(), 1);
    }
}
