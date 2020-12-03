use std::collections::HashMap;

use log::info;

use fluvio::{Fluvio, TopicProducer};

use crate::TestOption;
use super::message::*;

type Offsets = HashMap<String, i64>;

pub async fn produce_message(option: &TestOption) -> Offsets {
    use fluvio_future::task::spawn; // get initial offsets for each of the topic
    let offsets = offsets::find_offsets(&option).await;

    if option.use_cli() {
        cli::produce_message_with_cli(option, offsets.clone()).await;
    } else if option.consumer_wait {
        produce_message_with_api(offsets.clone(), option.clone()).await;
    } else {
        spawn(produce_message_with_api(offsets.clone(), option.clone()));
    }

    offsets
}

mod offsets {

    use std::collections::HashMap;

    use fluvio::{Fluvio, FluvioAdmin};

    use fluvio_controlplane_metadata::partition::PartitionSpec;
    use fluvio_controlplane_metadata::partition::ReplicaKey;

    use super::TestOption;

    pub async fn find_offsets(option: &TestOption) -> HashMap<String, i64> {
        let replication = option.replication();

        let mut offsets = HashMap::new();

        let client = Fluvio::connect().await.expect("should connect");
        let mut admin = client.admin().await;

        for i in 0..replication {
            let topic_name = option.topic_name(i);
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

pub async fn produce_message_with_api(offsets: Offsets, option: TestOption) {
    use std::time::Duration;
    use fluvio_future::timer::sleep;

    let client = Fluvio::connect().await.expect("should connect");
    let replication = option.replication();

    for i in 0..replication {
        let topic_name = option.topic_name(i);

        let base_offset = *offsets.get(&topic_name).expect("offsets");
        let producer = get_producer(&client, &topic_name).await;

        for i in 0..option.produce.produce_iteration {
            let offset = base_offset + i as i64;
            let message = generate_message(offset, &topic_name, &option);
            let len = message.len();
            info!("trying send: {}, iteration: {}", topic_name, i);
            producer
                .send_record(message, 0)
                .await
                .expect("message sent");
            info!(
                "completed send iter: {}, offset: {},len: {}",
                topic_name, offset, len
            );
            sleep(Duration::from_millis(10)).await;
        }
    }
}

mod cli {

    use std::io::Write;
    use std::process::Stdio;

    use fluvio_system_util::bin::get_fluvio;

    use crate::cli::TestOption;
    use crate::util::CommandUtil;

    use super::*;

    pub async fn produce_message_with_cli(option: &TestOption, offsets: Offsets) {
        println!("starting produce");

        let produce_count = option.produce.produce_iteration;
        for i in 0..produce_count {
            produce_message_replication(i, &offsets, option);
            //sleep(Duration::from_millis(10)).await
        }
    }

    fn produce_message_replication(iteration: u16, offsets: &Offsets, option: &TestOption) {
        let replication = option.replication();

        for i in 0..replication {
            produce_message_inner(iteration, &option.topic_name(i), offsets, option);
        }
    }

    fn produce_message_inner(
        _iteration: u16,
        topic_name: &str,
        offsets: &Offsets,
        option: &TestOption,
    ) {
        use std::io;

        let base_offset = *offsets.get(topic_name).expect("offsets");

        info!(
            "produce cli message: {}, offset: {}",
            topic_name, base_offset
        );

        let mut child = get_fluvio()
            .expect("no fluvio")
            .rust_log(option.client_log.as_deref())
            .stdin(Stdio::piped())
            .arg("produce")
            .arg(topic_name)
            .print()
            .spawn()
            .expect("no child");

        let stdin = child.stdin.as_mut().expect("Failed to open stdin");
        let msg = generate_message(base_offset, topic_name, option);
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
