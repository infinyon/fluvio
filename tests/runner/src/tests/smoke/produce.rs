use std::collections::HashMap;

use fluvio::Fluvio;
use fluvio_future::task::spawn;

use crate::TestOption;
use super::message::*;

type Offsets = HashMap<String, i64>;

pub async fn produce_message(option: &TestOption) -> Offsets {
    // get initial offsets for each of the topic
    let offsets = offsets::find_offsets(&option).await;

    if option.produce.produce_iteration == 1 {
        cli::produce_message_with_cli(option, offsets.clone()).await;
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

        let mut client = Fluvio::connect().await.expect("should connect");
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

pub async fn produce_message_with_api(offsets: Offsets, option: TestOption) {
    let client = Fluvio::connect().await.expect("should connect");
    let replication = option.replication();

    for i in 0..replication {
        let topic_name = option.topic_name(i);

        let base_offset = *offsets.get(&topic_name).expect("offsets");
        let producer = client.topic_producer(&topic_name).await.expect("producer");

        for i in 0..option.produce.produce_iteration {
            let offset = base_offset + i as i64;
            let message = generate_message(offset, &topic_name, &option);
            let len = message.len();
            producer
                .send_record(message, 0)
                .await
                .expect("message sent");
            println!(
                "produced message topic: {}, offset: {},len: {}",
                topic_name, offset, len
            );
        }
    }
}

mod cli {

    use std::io::Write;
    use std::process::Stdio;

    use utils::bin::get_fluvio;
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

        let mut child = get_fluvio()
            .expect("no fluvio")
            .rust_log(option.rust_log.as_deref())
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
