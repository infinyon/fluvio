use std::collections::HashMap;

use fluvio::FluvioAdmin;

use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use super::SmokeTestCase;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;

pub type Offsets = HashMap<String, i64>;

pub async fn find_offsets(test_driver: &TestDriver, test_case: &SmokeTestCase) -> Offsets {
    let partition = test_case.environment.partition;

    let _consumer_wait = test_case.option.consumer_wait;

    let mut offsets = HashMap::new();

    let mut admin = test_driver.client().admin().await;

    for _i in 0..partition {
        let topic_name = test_case.environment.base_topic_name();
        // find last offset
        let offset = last_leo(&mut admin, &topic_name).await;
        println!("found topic: {} offset: {}", &topic_name, offset);
        offsets.insert(topic_name.to_string(), offset);
    }

    offsets
}

async fn last_leo(admin: &mut FluvioAdmin, topic: &str) -> i64 {
    let partitions = admin
        .all::<PartitionSpec>()
        .await
        .expect("get partitions status");

    for partition in partitions {
        let replica: ReplicaKey = partition
            .name
            .clone()
            .try_into()
            .expect("cannot parse partition");

        if replica.topic == topic && replica.partition == 0 {
            return partition.status.leader.leo;
        }
    }

    panic!("cannot found partition 0 for topic: {topic}");
}
