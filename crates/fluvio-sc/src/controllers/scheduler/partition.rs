use std::{collections::BTreeMap, ops::Deref};

use tracing::{instrument, debug, trace};

use fluvio_controlplane_metadata::topic::{TopicReplicaParam, PartitionMaps};
use fluvio_stream_model::core::MetadataItem;
use fluvio_types::{PartitionId, SpuId, ReplicaMap, ReplicationFactor};

use crate::stores::{
    spu::{SpuLocalStore, SpuLocalStorePolicy},
    partition::{
        PartitionLocalStore, PartitionLocalStorePolicy, ReplicaSchedulingGroups, SpuWeightSelection,
    },
};

/// map of partition to spus
/// this is wrapper of replica map in order to make easier to conversion
#[derive(Default, Debug, PartialEq)]
pub(crate) struct ReplicaPartitionMap(ReplicaMap);

impl ReplicaPartitionMap {
    // check if there are resource are scheduled
    pub fn scheduled(&self) -> bool {
        !self.is_empty()
    }
}

impl From<ReplicaMap> for ReplicaPartitionMap {
    fn from(replica_map: ReplicaMap) -> Self {
        Self(replica_map)
    }
}

impl From<Vec<(PartitionId, Vec<SpuId>)>> for ReplicaPartitionMap {
    fn from(replica_map: Vec<(PartitionId, Vec<SpuId>)>) -> Self {
        Self(replica_map.into_iter().collect())
    }
}

impl From<&PartitionMaps> for ReplicaPartitionMap {
    fn from(partition_maps: &PartitionMaps) -> Self {
        let mut replica_map: ReplicaMap = BTreeMap::new();

        for partition in partition_maps.maps() {
            replica_map.insert(partition.id as PartitionId, partition.replicas.clone());
        }

        replica_map.into()
    }
}

impl From<ReplicaPartitionMap> for ReplicaMap {
    fn from(val: ReplicaPartitionMap) -> Self {
        val.0
    }
}

impl Deref for ReplicaPartitionMap {
    type Target = BTreeMap<PartitionId, Vec<SpuId>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Allocate partitions to spus
#[derive(Debug)]
pub(crate) struct PartitionScheduler<'a, C: MetadataItem> {
    spus: &'a SpuLocalStore<C>,
    partitions: &'a PartitionLocalStore<C>,
    scheduling_groups: ReplicaSchedulingGroups,
}

impl<'a, C> PartitionScheduler<'a, C>
where
    C: MetadataItem,
{
    pub(crate) async fn init(
        spus: &'a SpuLocalStore<C>,
        partitions: &'a PartitionLocalStore<C>,
    ) -> PartitionScheduler<'a, C> {
        let scheduling_groups = partitions.group_by_spu().await;
        Self {
            spus,
            partitions,
            scheduling_groups,
        }
    }

    pub(crate) fn spus(&self) -> &'a SpuLocalStore<C> {
        self.spus
    }

    pub(crate) fn partitions(&self) -> &'a PartitionLocalStore<C> {
        self.partitions
    }

    /// Generate replica map for a specific topic
    #[instrument(level = "debug")]
    pub async fn generate_replica_map_for_topic(
        &'a mut self,
        param: &TopicReplicaParam,
        actual_replica_map: Option<&ReplicaPartitionMap>,
    ) -> ReplicaPartitionMap {
        let spu_count = self.spus.count().await as ReplicationFactor;
        if spu_count < param.replication_factor {
            debug!(
                param.replication_factor,
                spu_count, "insufficient spu count"
            );
            ReplicaPartitionMap::default()
        } else {
            self.generate_partitions_without_rack(param, actual_replica_map)
                .await
        }
    }

    /// Generate partitions without taking rack assignments into consideration
    pub(crate) async fn generate_partitions_without_rack(
        &mut self,
        param: &TopicReplicaParam,
        actual_replica_map: Option<&ReplicaPartitionMap>,
    ) -> ReplicaPartitionMap {
        let mut online_spus = self.spus.online_spu_ids().await;
        online_spus.sort_unstable();

        trace!(?online_spus, "online");
        let mut partition_map = BTreeMap::new();
        for p_idx in 0..param.partitions {
            let mut reserved_spus: Vec<i32> = vec![]; // spu reserved

            // ensure we don't change old partitions for no reason
            if let Some(actual_replica_map) = actual_replica_map {
                if let Some(replicas) = actual_replica_map.get(&(p_idx as PartitionId)) {
                    if replicas.len() == param.replication_factor as usize {
                        partition_map.insert(p_idx as PartitionId, replicas.clone());
                        continue;
                    }
                }
            }

            for r_idx in 0..param.replication_factor {
                // for each replica, they must be on different spu, anti-affinity
                if let Some(spu) = self.scheduling_groups.find_suitable_spu(
                    &online_spus,
                    &reserved_spus,
                    if r_idx == 0 {
                        SpuWeightSelection::Leader
                    } else {
                        SpuWeightSelection::Follower
                    },
                ) {
                    trace!(spu, "found spu");
                    reserved_spus.push(spu);
                    if r_idx == 0 {
                        self.scheduling_groups.increase_leaders(spu);
                    } else {
                        self.scheduling_groups.increase_followers(spu);
                    }
                } else {
                    trace!("no suitable spu found");
                    return BTreeMap::new().into();
                }
            }
            partition_map.insert(p_idx as PartitionId, reserved_spus);
        }

        partition_map.into()
    }
}

//
// Unit Tests
//
#[cfg(test)]
pub mod replica_map_test {

    use crate::stores::{
        spu::{SpuAdminStore, DefaultSpuStore},
        partition::{PartitionAdminStore, DefaultPartitionStore},
    };

    use super::*;

    #[fluvio_future::test]
    async fn generate_replica_simple() {
        let spus = SpuAdminStore::quick(vec![(0, true, None), (1, true, None)]);
        let partitions = PartitionAdminStore::new_shared();
        assert_eq!(spus.online_spu_count().await, 2);

        let param = TopicReplicaParam {
            partitions: 2,
            replication_factor: 1,
            ignore_rack_assignment: false,
        };
        let mut scheduler = PartitionScheduler::init(&spus, &partitions).await;
        // this should be evenly distributed
        let expected: ReplicaPartitionMap = vec![(0, vec![0]), (1, vec![1])].into();
        assert_eq!(
            scheduler
                .generate_partitions_without_rack(&param, None)
                .await,
            expected
        );
    }

    #[fluvio_future::test]
    async fn generate_replica_partition_more_spu() {
        let spus = SpuAdminStore::quick(vec![(0, true, None), (1, true, None)]);
        let partitions = PartitionAdminStore::new_shared();

        let param = TopicReplicaParam {
            partitions: 4,
            replication_factor: 1,
            ignore_rack_assignment: false,
        };
        let mut scheduler = PartitionScheduler::init(&spus, &partitions).await;
        // not enough spus but leader should be scheduled
        let expected: ReplicaPartitionMap =
            vec![(0, vec![0]), (1, vec![1]), (2, vec![0]), (3, vec![1])].into();
        assert_eq!(
            scheduler
                .generate_partitions_without_rack(&param, None)
                .await,
            expected
        );
    }

    #[fluvio_future::test]
    async fn generate_replica_partition_not_enough_follower() {
        let spus = SpuAdminStore::quick(vec![(0, true, None), (1, true, None)]);
        let partitions = PartitionAdminStore::new_shared();

        let param = TopicReplicaParam {
            partitions: 4,
            replication_factor: 3,
            ignore_rack_assignment: false,
        };
        let mut scheduler = PartitionScheduler::init(&spus, &partitions).await;
        // not enough spus for replication
        assert!(scheduler
            .generate_partitions_without_rack(&param, None)
            .await
            .is_empty());
    }

    #[fluvio_future::test]
    async fn generate_replica_map_for_topic_1x_replicas() {
        let spus = SpuAdminStore::quick(vec![
            (0, true, None),
            (1, true, None),
            (2, true, None),
            (4, true, None),
            (10, true, None),
        ]);
        let partitions = PartitionAdminStore::new_shared();

        assert_eq!(spus.online_spu_count().await, 5);

        let param = TopicReplicaParam {
            partitions: 4,
            replication_factor: 1,
            ignore_rack_assignment: false,
        };
        let mut scheduler = PartitionScheduler::init(&spus, &partitions).await;
        // evenly distributed
        let expected: ReplicaPartitionMap =
            vec![(0, vec![0]), (1, vec![1]), (2, vec![2]), (3, vec![4])].into();
        assert_eq!(
            scheduler
                .generate_partitions_without_rack(&param, None)
                .await,
            expected
        );
    }

    #[fluvio_future::test]
    async fn generate_replica_map_for_topic_2x_replicas() {
        let spus = DefaultSpuStore::quick(vec![
            (0, true, None),
            (1, true, None),
            (2, true, None),
            (3, true, None),
        ]);

        // load up existing partitions
        let partitions = DefaultPartitionStore::bulk_load(vec![
            (("t1", 0), vec![0, 1, 2]),
            (("t1", 1), vec![1, 2]),
            (("t2", 0), vec![0, 3]),
        ]);

        let mut scheduler = PartitionScheduler::init(&spus, &partitions).await;

        // test 4 partitions, 2 replicas - index 3
        let param = TopicReplicaParam {
            partitions: 5,
            replication_factor: 3,
            ignore_rack_assignment: false,
        };

        //println!("before group: {:#?}", scheduler.scheduling_groups());

        let expect: ReplicaPartitionMap = vec![
            (0, vec![2, 0, 1]),
            (1, vec![3, 0, 1]),
            (2, vec![1, 3, 0]), // round back to 0 since everything is fairly allocated now
            (3, vec![2, 3, 0]),
            (4, vec![3, 2, 1]),
        ]
        .into();

        let actual = scheduler
            .generate_partitions_without_rack(&param, None)
            .await;
        //println!("after group: {:#?}", scheduler.scheduling_groups());

        assert_eq!(actual, expect);
    }
}
