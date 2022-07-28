use std::collections::HashMap;

use fluvio::FluvioAdmin;

use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use super::SmokeTestCase;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use tracing::{instrument, Instrument, debug_span, trace_span};

pub type Offsets = HashMap<String, i64>;

#[instrument(skip(test_driver))]
pub async fn find_offsets(test_driver: &TestDriver, test_case: &SmokeTestCase) -> Offsets {
    let partition = test_case.environment.partition;

    let _consumer_wait = test_case.option.consumer_wait;

    let mut offsets = HashMap::new();

    let mut admin = test_driver
        .client()
        .admin()
        .instrument(trace_span!("fluvio_admin"))
        .await;

    for i in 0..partition {
        let topic_name = test_case.environment.base_topic_name();
        // find last offset
        let offset = last_leo(&mut admin, &topic_name)
            .instrument(debug_span!("last_leo", partition = i))
            .await;
        println!("found topic: {} offset: {}", &topic_name, offset);
        offsets.insert(topic_name.to_string(), offset);
    }

    offsets
}

#[instrument(skip(admin))]
async fn last_leo(admin: &mut FluvioAdmin, topic: &str) -> i64 {
    let partitions = admin
        .list::<PartitionSpec, _>(vec![])
        .instrument(trace_span!("list_partition"))
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

    panic!("cannot found partition 0 for topic: {}", topic);
}
