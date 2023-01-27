use std::fmt;
use std::collections::BTreeMap;

use tracing::{debug, trace, instrument};
use rand::thread_rng;
use rand::Rng;

use fluvio_types::*;
use fluvio_controlplane_metadata::topic::*;

use crate::stores::topic::*;
use crate::stores::partition::*;
use crate::stores::spu::*;

//
/// Validate assigned topic spec parameters and update topic status
///  * error is passed to the topic reason.
///
pub fn validate_assigned_topic_parameters(partition_map: &PartitionMaps) -> TopicNextState {
    if let Err(err) = partition_map.valid_partition_map() {
        TopicStatus::next_resolution_invalid_config(err.to_string()).into()
    } else {
        TopicStatus::next_resolution_pending().into()
    }
}

///
/// Validate computed topic spec parameters and update topic status
///  * error is passed to the topic reason.
///
pub fn validate_computed_topic_parameters(param: &TopicReplicaParam) -> TopicNextState {
    if let Err(err) = ReplicaSpec::valid_partition(&param.partitions) {
        TopicStatus::next_resolution_invalid_config(err.to_string()).into()
    } else if let Err(err) = ReplicaSpec::valid_replication_factor(&param.replication_factor) {
        TopicStatus::next_resolution_invalid_config(err.to_string()).into()
    } else {
        TopicStatus::next_resolution_pending().into()
    }
}

///
/// Generate Replica Map if there are enough online spus
///  * returns a replica map or a reason for the failure
///  * fatal error  configuration errors and are not recoverable
///
#[instrument(level = "trace", skip(spus, param))]
pub async fn generate_replica_map(
    spus: &SpuAdminStore,
    param: &TopicReplicaParam,
) -> TopicNextState {
    let spu_count = spus.count().await as ReplicationFactor;
    if spu_count < param.replication_factor {
        trace!(
            "R-MAP needs {:?} online spus, found {:?}",
            param.replication_factor,
            spu_count
        );

        let reason = format!("need {} more SPU", param.replication_factor - spu_count);
        TopicStatus::set_resolution_no_resource(reason).into()
    } else {
        let replica_map = generate_replica_map_for_topic(spus, param, None).await;
        if !replica_map.is_empty() {
            (TopicStatus::next_resolution_provisioned(), replica_map).into()
        } else {
            let reason = "empty replica map";
            TopicStatus::set_resolution_no_resource(reason.to_owned()).into()
        }
    }
}

///
/// Compare assigned SPUs versus local SPUs. If all assigned SPUs are live,
/// update topic status to ok. otherwise, mark as waiting for live SPUs
///
#[instrument(skip(partition_maps, spu_store))]
pub async fn update_replica_map_for_assigned_topic(
    partition_maps: &PartitionMaps,
    spu_store: &SpuAdminStore,
) -> TopicNextState {
    let partition_map_spus = partition_maps.unique_spus_in_partition_map();
    let spus_id = spu_store.spu_ids().await;

    // ensure spu exists
    for spu in &partition_map_spus {
        if !spus_id.contains(spu) {
            return TopicStatus::next_resolution_invalid_config(format!("invalid spu id: {spu}"))
                .into();
        }
    }

    let replica_map = partition_maps.partition_map_to_replica_map();
    if replica_map.is_empty() {
        TopicStatus::next_resolution_invalid_config("invalid replica map".to_owned()).into()
    } else {
        (TopicStatus::next_resolution_provisioned(), replica_map).into()
    }
}

/// values for next state
#[derive(Default, Debug)]
pub struct TopicNextState {
    pub resolution: TopicResolution,
    pub reason: String,
    pub replica_map: ReplicaMap,
    pub partitions: Vec<PartitionAdminMd>,
}

impl fmt::Display for TopicNextState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

impl From<(TopicResolution, String)> for TopicNextState {
    fn from(val: (TopicResolution, String)) -> Self {
        let (resolution, reason) = val;
        Self {
            resolution,
            reason,
            ..Default::default()
        }
    }
}

impl From<((TopicResolution, String), ReplicaMap)> for TopicNextState {
    fn from(val: ((TopicResolution, String), ReplicaMap)) -> Self {
        let ((resolution, reason), replica_map) = val;
        Self {
            resolution,
            reason,
            replica_map,
            ..Default::default()
        }
    }
}

impl From<((TopicResolution, String), Vec<PartitionAdminMd>)> for TopicNextState {
    fn from(val: ((TopicResolution, String), Vec<PartitionAdminMd>)) -> Self {
        let ((resolution, reason), partitions) = val;
        Self {
            resolution,
            reason,
            partitions,
            ..Default::default()
        }
    }
}

impl TopicNextState {
    /// apply this state to topic and return set of partitions
    pub fn apply_as_next_state(self, topic: &mut TopicAdminMd) -> Vec<PartitionAdminMd> {
        topic.status.resolution = self.resolution;
        topic.status.reason = self.reason;
        if !self.replica_map.is_empty() {
            topic.status.set_replica_map(self.replica_map);
        }
        self.partitions
    }

    /// create same next state as given topic
    pub fn same_next_state(topic: &TopicAdminMd) -> TopicNextState {
        TopicNextState {
            resolution: topic.status.resolution.clone(),
            ..Default::default()
        }
    }

    /// given topic, compute next state
    pub async fn compute_next_state(
        topic: &TopicAdminMd,
        spu_store: &SpuAdminStore,
        partition_store: &PartitionAdminStore,
    ) -> TopicNextState {
        match topic.spec().replicas() {
            // Computed Topic
            ReplicaSpec::Computed(ref param) => match topic.status.resolution {
                TopicResolution::Init | TopicResolution::InvalidConfig => {
                    validate_computed_topic_parameters(param)
                }
                TopicResolution::Pending | TopicResolution::InsufficientResources => {
                    let mut next_state = generate_replica_map(spu_store, param).await;
                    if next_state.resolution == TopicResolution::Provisioned {
                        debug!(
                            "Topic: {} replica generate success, status is provisioned",
                            topic.key()
                        );
                        next_state.partitions = topic.create_new_partitions(partition_store).await;
                    }
                    next_state
                }
                _ => {
                    debug!(
                        "topic: {} resolution: {:#?} ignoring",
                        topic.key, topic.status.resolution
                    );
                    let mut next_state = TopicNextState::same_next_state(topic);
                    if next_state.resolution == TopicResolution::Provisioned {
                        next_state.partitions = topic.create_new_partitions(partition_store).await;
                    }
                    next_state
                }
            },

            // Assign Topic
            ReplicaSpec::Assigned(ref partition_map) => match topic.status.resolution {
                TopicResolution::Init | TopicResolution::InvalidConfig => {
                    validate_assigned_topic_parameters(partition_map)
                }
                TopicResolution::Pending | TopicResolution::InsufficientResources => {
                    let mut next_state =
                        update_replica_map_for_assigned_topic(partition_map, spu_store).await;
                    if next_state.resolution == TopicResolution::Provisioned {
                        next_state.partitions = topic.create_new_partitions(partition_store).await;
                    }
                    next_state
                }
                _ => {
                    debug!(
                        "assigned topic: {} resolution: {:#?} ignoring",
                        topic.key, topic.status.resolution
                    );
                    let mut next_state = TopicNextState::same_next_state(topic);
                    if next_state.resolution == TopicResolution::Provisioned {
                        next_state.partitions = topic.create_new_partitions(partition_store).await;
                    }
                    next_state
                }
            },
        }
    }
}

///
/// Generate replica map for a specific topic
///
#[instrument(level = "trace", skip(spus, param, from_index))]
pub async fn generate_replica_map_for_topic(
    spus: &SpuAdminStore,
    param: &TopicReplicaParam,
    from_index: Option<u32>,
) -> ReplicaMap {
    let in_rack_count = spus.spus_in_rack_count().await;

    // generate partition map (with our without rack assignment)
    if param.ignore_rack_assignment || in_rack_count == 0 {
        generate_partitions_without_rack(spus, param, from_index).await
    } else {
        generate_partitions_with_rack_assignment(spus, param, from_index).await
    }
}

///
/// Generate partitions on spus that have been assigned to racks
///
async fn generate_partitions_with_rack_assignment(
    spus: &SpuAdminStore,
    param: &TopicReplicaParam,
    start_index: Option<u32>,
) -> ReplicaMap {
    let mut partition_map: ReplicaMap = BTreeMap::new();
    let rack_map = SpuAdminStore::live_spu_rack_map_sorted(spus).await;
    let spu_list = SpuAdminStore::online_spus_in_rack(&rack_map);
    let spu_cnt = spus.online_spu_count().await;

    let s_idx = start_index.unwrap_or_else(|| thread_rng().gen_range(0..spu_cnt));

    for p_idx in 0..param.partitions {
        let mut replicas: Vec<i32> = vec![];
        for r_idx in 0..param.replication_factor {
            let spu_idx = ((s_idx + p_idx + r_idx) % spu_cnt) as usize;
            replicas.push(spu_list[spu_idx]);
        }
        partition_map.insert(p_idx as PartitionId, replicas);
    }

    partition_map
}

///
/// Generate partitions without taking rack assignments into consideration
///
async fn generate_partitions_without_rack(
    spus: &SpuAdminStore,
    param: &TopicReplicaParam,
    start_index: Option<u32>,
) -> ReplicaMap {
    let mut partition_map: ReplicaMap = BTreeMap::new();
    let spu_cnt = spus.spu_used_for_replica().await as u32;
    let spu_ids = spus.spu_ids().await;

    let s_idx: u32 = start_index.unwrap_or_else(|| thread_rng().gen_range(0..spu_cnt));

    let gap_max = spu_cnt - param.replication_factor + 1;
    for p_idx in 0..param.partitions {
        let mut replicas: Vec<i32> = vec![];
        let gap_cnt = ((s_idx + p_idx) / spu_cnt) % gap_max;
        for r_idx in 0..param.replication_factor {
            let gap = if r_idx != 0 { gap_cnt } else { 0 };
            let spu_idx = ((s_idx + p_idx + r_idx + gap) % spu_cnt) as usize;
            replicas.push(spu_ids[spu_idx]);
        }
        partition_map.insert(p_idx as PartitionId, replicas);
    }

    partition_map
}

//
// Unit Tests
//
#[cfg(test)]
pub mod replica_map_test {

    use std::collections::BTreeMap;

    use fluvio_controlplane_metadata::spu::store::SpuLocalStorePolicy;

    use super::*;

    #[fluvio_future::test]
    async fn generate_replica_map_for_topic_1x_replicas_no_rack() {
        let spus = SpuAdminStore::quick(vec![
            (0, true, None),
            (1, true, None),
            (2, true, None),
            (4, true, None),
            (5000, true, None),
        ]);

        assert_eq!(spus.online_spu_count().await, 5);

        // test 4 partitions, 1 replicas - index 8
        let param = (4, 1, false).into();
        let map_1xi = generate_replica_map_for_topic(&spus, &param, Some(8)).await;
        let mut map_1xi_expected = BTreeMap::new();
        map_1xi_expected.insert(0, vec![4]);
        map_1xi_expected.insert(1, vec![5000]);
        map_1xi_expected.insert(2, vec![0]);
        map_1xi_expected.insert(3, vec![1]);
        assert_eq!(map_1xi, map_1xi_expected);
    }

    #[fluvio_future::test]
    async fn generate_replica_map_for_topic_2x_replicas_no_rack() {
        let spus = SpuAdminStore::quick(vec![
            (0, true, None),
            (1, true, None),
            (2, true, None),
            (3, true, None),
            (4, true, None),
        ]);

        // test 4 partitions, 2 replicas - index 3
        let param = (4, 2, false).into();
        let map_2xi = generate_replica_map_for_topic(&spus, &param, Some(3)).await;
        let mut map_2xi_expected = BTreeMap::new();
        map_2xi_expected.insert(0, vec![3, 4]);
        map_2xi_expected.insert(1, vec![4, 0]);
        map_2xi_expected.insert(2, vec![0, 2]);
        map_2xi_expected.insert(3, vec![1, 3]);
        assert_eq!(map_2xi, map_2xi_expected);
    }

    #[fluvio_future::test]
    async fn generate_replica_map_for_topic_3x_replicas_no_rack() {
        let spus = SpuAdminStore::quick(vec![
            (0, true, None),
            (1, true, None),
            (2, true, None),
            (3, true, None),
            (4, true, None),
        ]);

        // test 21 partitions, 3 replicas - index 0
        let param = (21, 3, false).into();
        let map_3x = generate_replica_map_for_topic(&spus, &param, Some(0)).await;
        let mut map_3x_expected = BTreeMap::new();
        map_3x_expected.insert(0, vec![0, 1, 2]);
        map_3x_expected.insert(1, vec![1, 2, 3]);
        map_3x_expected.insert(2, vec![2, 3, 4]);
        map_3x_expected.insert(3, vec![3, 4, 0]);
        map_3x_expected.insert(4, vec![4, 0, 1]);
        map_3x_expected.insert(5, vec![0, 2, 3]);
        map_3x_expected.insert(6, vec![1, 3, 4]);
        map_3x_expected.insert(7, vec![2, 4, 0]);
        map_3x_expected.insert(8, vec![3, 0, 1]);
        map_3x_expected.insert(9, vec![4, 1, 2]);
        map_3x_expected.insert(10, vec![0, 3, 4]);
        map_3x_expected.insert(11, vec![1, 4, 0]);
        map_3x_expected.insert(12, vec![2, 0, 1]);
        map_3x_expected.insert(13, vec![3, 1, 2]);
        map_3x_expected.insert(14, vec![4, 2, 3]);
        map_3x_expected.insert(15, vec![0, 1, 2]);
        map_3x_expected.insert(16, vec![1, 2, 3]);
        map_3x_expected.insert(17, vec![2, 3, 4]);
        map_3x_expected.insert(18, vec![3, 4, 0]);
        map_3x_expected.insert(19, vec![4, 0, 1]);
        map_3x_expected.insert(20, vec![0, 2, 3]);
        assert_eq!(map_3x, map_3x_expected);

        // test 4 partitions, 3 replicas - index 12
        let param = (4, 3, false).into();
        let map_3xi = generate_replica_map_for_topic(&spus, &param, Some(12)).await;
        let mut map_3xi_expected = BTreeMap::new();
        map_3xi_expected.insert(0, vec![2, 0, 1]);
        map_3xi_expected.insert(1, vec![3, 1, 2]);
        map_3xi_expected.insert(2, vec![4, 2, 3]);
        map_3xi_expected.insert(3, vec![0, 1, 2]);
        assert_eq!(map_3xi, map_3xi_expected);
    }

    #[fluvio_future::test]
    async fn generate_replica_map_for_topic_4x_replicas_no_rack() {
        let spus = SpuAdminStore::quick(vec![
            (0, true, None),
            (1, true, None),
            (2, true, None),
            (3, true, None),
            (4, true, None),
        ]);

        // test 4 partitions, 4 replicas - index 10
        let param = (4, 4, false).into();
        let map_4xi = generate_replica_map_for_topic(&spus, &param, Some(10)).await;
        let mut map_4xi_expected = BTreeMap::new();
        map_4xi_expected.insert(0, vec![0, 1, 2, 3]);
        map_4xi_expected.insert(1, vec![1, 2, 3, 4]);
        map_4xi_expected.insert(2, vec![2, 3, 4, 0]);
        map_4xi_expected.insert(3, vec![3, 4, 0, 1]);
        assert_eq!(map_4xi, map_4xi_expected);
    }

    #[fluvio_future::test]
    async fn generate_replica_map_for_topic_5x_replicas_no_rack() {
        let spus = SpuAdminStore::quick(vec![
            (0, true, None),
            (1, true, None),
            (3, true, None),
            (4, true, None),
            (5002, true, None),
        ]);

        // test 4 partitions, 5 replicas - index 14
        let param = (4, 5, false).into();
        let map_5xi = generate_replica_map_for_topic(&spus, &param, Some(14)).await;
        let mut map_5xi_expected = BTreeMap::new();
        map_5xi_expected.insert(0, vec![5002, 0, 1, 3, 4]);
        map_5xi_expected.insert(1, vec![0, 1, 3, 4, 5002]);
        map_5xi_expected.insert(2, vec![1, 3, 4, 5002, 0]);
        map_5xi_expected.insert(3, vec![3, 4, 5002, 0, 1]);
        assert_eq!(map_5xi, map_5xi_expected);
    }

    #[fluvio_future::test]
    async fn generate_replica_map_for_topic_6_part_3_rep_6_brk_3_rak() {
        let r1 = String::from("r1");
        let r2 = String::from("r2");
        let r3 = String::from("r3");

        let spus = SpuAdminStore::quick(vec![
            (0, true, Some(r1.clone())),
            (1, true, Some(r2.clone())),
            (2, true, Some(r2.clone())),
            (3, true, Some(r3.clone())),
            (4, true, Some(r3.clone())),
            (5, true, Some(r3.clone())),
        ]);

        // Compute & compare with result
        let param = (6, 3, false).into();
        let computed = generate_replica_map_for_topic(&spus, &param, Some(0)).await;
        let mut expected = BTreeMap::new();
        expected.insert(0, vec![3, 2, 0]);
        expected.insert(1, vec![2, 0, 4]);
        expected.insert(2, vec![0, 4, 1]);
        expected.insert(3, vec![4, 1, 5]);
        expected.insert(4, vec![1, 5, 3]);
        expected.insert(5, vec![5, 3, 2]);

        assert_eq!(computed, expected);
    }

    #[fluvio_future::test]
    async fn generate_replica_map_for_topic_12_part_4_rep_11_brk_4_rak() {
        let r1 = String::from("r1");
        let r2 = String::from("r2");
        let r3 = String::from("r3");
        let r4 = String::from("r4");

        let spus = SpuAdminStore::quick(vec![
            (0, true, Some(r1.clone())),
            (1, true, Some(r1.clone())),
            (2, true, Some(r1.clone())),
            (3, true, Some(r2.clone())),
            (4, true, Some(r2.clone())),
            (5, true, Some(r2.clone())),
            (6, true, Some(r3.clone())),
            (7, true, Some(r3.clone())),
            (8, true, Some(r3.clone())),
            (9, true, Some(r4.clone())),
            (10, true, Some(r4.clone())),
            (11, true, Some(r4.clone())),
        ]);

        // Compute & compare with result
        let param = (12, 4, false).into();
        let computed = generate_replica_map_for_topic(&spus, &param, Some(0)).await;
        let mut expected = BTreeMap::new();
        expected.insert(0, vec![0, 4, 8, 9]);
        expected.insert(1, vec![4, 8, 9, 1]);
        expected.insert(2, vec![8, 9, 1, 5]);
        expected.insert(3, vec![9, 1, 5, 6]);
        expected.insert(4, vec![1, 5, 6, 10]);
        expected.insert(5, vec![5, 6, 10, 2]);
        expected.insert(6, vec![6, 10, 2, 3]);
        expected.insert(7, vec![10, 2, 3, 7]);
        expected.insert(8, vec![2, 3, 7, 11]);
        expected.insert(9, vec![3, 7, 11, 0]);
        expected.insert(10, vec![7, 11, 0, 4]);
        expected.insert(11, vec![11, 0, 4, 8]);

        assert_eq!(computed, expected);
    }

    #[fluvio_future::test]
    async fn generate_replica_map_for_topic_9_part_3_rep_9_brk_3_rak() {
        let r1 = String::from("r1");
        let r2 = String::from("r2");
        let r3 = String::from("r3");

        let spus = SpuAdminStore::quick(vec![
            (0, true, Some(r1.clone())),
            (1, true, Some(r1.clone())),
            (2, true, Some(r1.clone())),
            (3, true, Some(r2.clone())),
            (4, true, Some(r2.clone())),
            (5, true, Some(r2.clone())),
            (6, true, Some(r3.clone())),
            (7, true, Some(r3.clone())),
            (8, true, Some(r3.clone())),
        ]);

        // test 9 partitions, 3 replicas - index 0
        let param = (9, 3, false).into();
        let computed = generate_replica_map_for_topic(&spus, &param, Some(0)).await;
        let mut expected = BTreeMap::new();
        expected.insert(0, vec![0, 4, 8]);
        expected.insert(1, vec![4, 8, 1]);
        expected.insert(2, vec![8, 1, 5]);
        expected.insert(3, vec![1, 5, 6]);
        expected.insert(4, vec![5, 6, 2]);
        expected.insert(5, vec![6, 2, 3]);
        expected.insert(6, vec![2, 3, 7]);
        expected.insert(7, vec![3, 7, 0]);
        expected.insert(8, vec![7, 0, 4]);

        assert_eq!(computed, expected);
    }
}
