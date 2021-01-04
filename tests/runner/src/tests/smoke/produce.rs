use std::collections::HashMap;

use log::info;

use fluvio::{Fluvio};
use fluvio_command::CommandExt;


use crate::TestOption;
use super::message::*;

type Offsets = HashMap<String, i64>;

pub async fn produce_message(option: &TestOption) -> Offsets {
    let offsets = offsets::find_offsets().await;

    if option.use_cli() {
        cli::produce_message_with_cli(option, &offsets).await;
    } else {
        produce_message_with_api(&offsets, option).await;
    }

    offsets
}

mod offsets {

    use std::collections::HashMap;

    use fluvio::{Fluvio};

    use fluvio_controlplane_metadata::partition::PartitionSpec;
    use fluvio_controlplane_metadata::partition::ReplicaKey;

    pub async fn find_offsets() -> HashMap<String, i64> {
        use std::convert::TryInto;

        let mut offsets = HashMap::new();

        println!("reading existing topics offsets");
        let client = Fluvio::connect().await.expect("should connect");
        let mut admin = client.admin().await;

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

            if replica.partition == 0 {
                offsets.insert(replica.topic, partition.status.leader.leo);
            }
        }

        offsets
    }
}

pub async fn produce_message_with_api(offsets: &Offsets, option: &TestOption) {
    use fluvio_future::task::spawn; // get initial offsets for each of the topic

    for t in 0..option.topics() {
        let topic = option.topic_name(t);
        let base_offset = *offsets.get(&topic).expect("offsets");

        let produce = produce_message_for_topic(topic, option.clone(), base_offset);

        if option.consumer_wait {
            produce.await
        } else {
            spawn(produce);
        }
    }
}

/// produce using separate client
async fn produce_message_for_topic(topic: String, option: TestOption, base_offset: i64) {
    use std::time::Instant;

    let client = Fluvio::connect().await.expect("should connect");
    let producer = client
        .topic_producer(&topic)
        .await
        .expect("can't get producer");

    println!(
        "starting produce for topic: {}, iterations: {}, base_offset: {}",
        topic, option.produce.produce_iteration, base_offset
    );
    for i in 0..option.produce.produce_iteration {
        let offset = base_offset + i as i64;
        let message = generate_message(offset, &topic, &option);
        let len = message.len();
        info!("trying send: {}, iteration: {}", topic, i);

        let now = Instant::now();
        if let Err(err) = producer.send_record(message, 0).await {
            panic!(
                "send record error: {:#?} for topic: {} iteration: {}, elapsed: {}",
                err, topic, i, now.elapsed().as_millis()
            )
        }

        info!(
            "completed send iter: {}, offset: {},len: {}",
            topic, offset, len
        );
    }
    println!(
        "completed produce for topic: {}, iterations: {}",
        topic, option.produce.produce_iteration
    );
}

mod cli {
    use super::*;

    use std::io::Write;
    use std::process::Stdio;
    use crate::cli::TestOption;
    use fluvio_system_util::bin::get_fluvio;

    pub async fn produce_message_with_cli(option: &TestOption, offsets: &Offsets) {
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

        let mut child = get_fluvio().expect("no fluvio");
        if let Some(log) = &option.client_log {
            child.env("RUST_LOG", log);
        }
        child.stdin(Stdio::piped()).arg("produce").arg(topic_name);
        println!("Executing: {}", child.display());
        let mut child = child.spawn().expect("no child");

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
