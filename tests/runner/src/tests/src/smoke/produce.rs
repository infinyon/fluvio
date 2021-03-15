use std::collections::HashMap;

use log::info;

use super::SmokeTestCase;
use super::message::*;
use fluvio::{Fluvio, TopicProducer};
use fluvio_command::CommandExt;
use std::sync::Arc;

type Offsets = HashMap<String, i64>;

pub async fn produce_message(client: Arc<Fluvio>, test_case: &SmokeTestCase) -> Offsets {
    use fluvio_future::task::spawn; // get initial offsets for each of the topic
    let offsets = offsets::find_offsets(&test_case).await;

    let use_cli = test_case.option.use_cli;
    let consumer_wait = test_case.option.consumer_wait;

    if use_cli {
        cli::produce_message_with_cli(test_case, offsets.clone()).await;
    } else if consumer_wait {
        produce_message_with_api(client, offsets.clone(), test_case.clone()).await;
    } else {
        spawn(produce_message_with_api(
            client,
            offsets.clone(),
            test_case.clone(),
        ));
    }

    offsets
}

mod offsets {

    use std::collections::HashMap;

    use fluvio::{Fluvio, FluvioAdmin};

    use fluvio_controlplane_metadata::partition::PartitionSpec;
    use fluvio_controlplane_metadata::partition::ReplicaKey;

    use super::SmokeTestCase;

    pub async fn find_offsets(test_case: &SmokeTestCase) -> HashMap<String, i64> {
        let replication = test_case.environment.replication;

        let _consumer_wait = test_case.option.consumer_wait;

        let mut offsets = HashMap::new();

        let client = Fluvio::connect().await.expect("should connect");
        let mut admin = client.admin().await;

        for _i in 0..replication {
            let topic_name = test_case.environment.topic_name.clone();
            // find last offset
            let offset = last_leo(&mut admin, &topic_name).await;
            println!("found topic: {} offset: {}", topic_name, offset);
            offsets.insert(topic_name, offset);
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

async fn get_producer(client: &Fluvio, topic: &str) -> TopicProducer {
    use std::time::Duration;
    use fluvio_future::timer::sleep;

    for _ in 0..10 {
        match client.topic_producer(topic).await {
            Ok(client) => return client,
            Err(err) => {
                println!(
                    "unable to get producer to topic: {}, error: {} sleeping 10 second ",
                    topic, err
                );
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    panic!("can't get producer");
}

pub async fn produce_message_with_api(
    client: Arc<Fluvio>,
    offsets: Offsets,
    test_case: SmokeTestCase,
) {
    use std::time::Duration;
    use fluvio_future::timer::sleep;

    let replication = test_case.environment.replication;

    let produce_iteration = test_case.option.producer_iteration;

    for r in 0..replication {
        let topic_name = test_case.environment.topic_name.clone();

        let base_offset = *offsets.get(&topic_name).expect("offsets");
        let producer = get_producer(&client, &topic_name).await;

        for i in 0..produce_iteration {
            let offset = base_offset + i as i64;
            let message = generate_message(offset, &test_case);
            let len = message.len();
            info!("trying send: {}, iteration: {}", topic_name, i);
            producer.send_record(message, 0).await.unwrap_or_else(|_| {
                panic!("send record failed for replication: {} iteration: {}", r, i)
            });

            info!(
                "completed send iter: {}, offset: {},len: {}",
                topic_name, offset, len
            );
            sleep(Duration::from_millis(10)).await;
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
        let msg = generate_message(base_offset, test_case);
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
