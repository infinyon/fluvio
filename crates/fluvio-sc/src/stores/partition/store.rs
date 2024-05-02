//!
//! # Partition & Partitions Metadata
//!
//! Partition metadata information on cached in the local Controller.
//!

use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use tracing::debug;
use async_trait::async_trait;

use fluvio_controlplane::replica::Replica;
use fluvio_controlplane::replica::ReplicaLeader;
use fluvio_stream_model::core::MetadataItem;

use fluvio_stream_model::store::LocalStore;
use fluvio_types::PartitionId;
use fluvio_types::SpuId;
use tracing::trace;

use super::*;
use super::policy::ElectionPolicy;
use super::policy::ElectionScoring;

pub type SharedPartitionStore<C> = Arc<PartitionLocalStore<C>>;

pub type PartitionLocalStore<C> = LocalStore<PartitionSpec, C>;
pub type DefaultPartitionMd = PartitionMetadata<String>;
pub type DefaultPartitionStore = PartitionLocalStore<u32>;

#[allow(dead_code)]
pub(crate) trait PartitionMd<C: MetadataItem> {
    fn with_replicas(key: ReplicaKey, replicas: Vec<SpuId>) -> Self;

    fn quick(partition: ((impl Into<String>, PartitionId), Vec<SpuId>)) -> Self;
}

impl<C: MetadataItem> PartitionMd<C> for PartitionMetadata<C> {
    /// create new partition with replica map.
    /// first element of replicas is leader
    fn with_replicas(key: ReplicaKey, replicas: Vec<SpuId>) -> Self {
        let spec: PartitionSpec = replicas.into();
        Self::new(key, spec, PartitionStatus::default())
    }

    fn quick(partition: ((impl Into<String>, PartitionId), Vec<SpuId>)) -> Self {
        let (replica_key, replicas) = partition;
        Self::with_replicas(replica_key.into(), replicas)
    }
}

#[allow(dead_code)]
#[async_trait]
pub(crate) trait PartitionLocalStorePolicy<C>
where
    C: MetadataItem,
{
    async fn names(&self) -> Vec<ReplicaKey>;

    async fn topic_partitions(&self, topic: &str) -> Vec<PartitionMetadata<C>>;

    /// find all partitions that has spu in the replicas
    async fn partition_spec_for_spu(&self, target_spu: SpuId) -> Vec<(ReplicaKey, PartitionSpec)>;

    async fn count_topic_partitions(&self, topic: &str) -> i32;

    // return partitions that belong to this topic
    async fn topic_partitions_list(&self, topic: &str) -> Vec<ReplicaKey>;

    async fn table_fmt(&self) -> String;

    /// replica msg for target spu
    async fn replica_for_spu(&self, target_spu: SpuId) -> Vec<Replica>;

    async fn leaders(&self) -> Vec<ReplicaLeader>;

    fn bulk_load(partitions: Vec<((impl Into<String>, PartitionId), Vec<SpuId>)>) -> Self;

    /// group replicas by spu
    async fn group_by_spu(&self) -> ReplicaSchedulingGroups;
}

/// List of Replica groups for scheduling
#[derive(Debug, Default)]
pub(crate) struct ReplicaSchedulingGroups(HashMap<SpuId, PartitionCountSpu>);

impl Deref for ReplicaSchedulingGroups {
    type Target = HashMap<SpuId, PartitionCountSpu>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ReplicaSchedulingGroups {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ReplicaSchedulingGroups {
    /// find suitable leader
    /// this is done by scanning all spu and find the one with least weight
    pub(crate) fn find_suitable_spu(
        &self,
        spu_list: &Vec<SpuId>,
        anti_affinity: &Vec<SpuId>,
        weight: SpuWeightSelection,
    ) -> Option<SpuId> {
        trace!(?spu_list, ?anti_affinity, "find_suitable_spu");

        let mut current_spu: Option<SpuId> = None;
        let mut min_weight = u16::MAX;
        for spu_id in spu_list {
            // check for anti-affinity
            if anti_affinity.contains(spu_id) {
                continue;
            }
            let candidate_weight = if let Some(group) = self.get(spu_id) {
                match weight {
                    SpuWeightSelection::Leader => group.leader_weight(),
                    SpuWeightSelection::Follower => group.follower_weight(),
                }
            } else {
                0
            };

            if candidate_weight < min_weight {
                min_weight = candidate_weight;
                current_spu = Some(*spu_id);
            }
        }

        current_spu
    }

    pub(crate) fn increase_leaders(&mut self, spu: SpuId) {
        if let Some(groups) = self.get_mut(&spu) {
            groups.leaders += 1;
        } else {
            let mut groups = PartitionCountSpu::default();
            groups.leaders += 1;
            self.insert(spu, groups);
        }
    }

    pub(crate) fn increase_followers(&mut self, spu: SpuId) {
        if let Some(groups) = self.get_mut(&spu) {
            groups.followers += 1;
        } else {
            let mut new_group = PartitionCountSpu::default();
            new_group.followers += 1;
            self.insert(spu, new_group);
        }
    }

    /*
    pub(crate) fn insert_leader(&mut self, spu: SpuId, partition: ReplicaKey) {
        if let Some(groups) = self.get_mut(&spu) {
            groups.leaders.insert(partition);
        } else {
            let mut new_group = PartitionBySpu::default();
            new_group.leaders.insert(partition);
            self.insert(spu, new_group);
        }
    }

    pub(crate) fn insert_follower(&mut self, spu: SpuId, partition: ReplicaKey) {
        if let Some(groups) = self.get_mut(&spu) {
            groups.followers.insert(partition);
        } else {
            let mut new_group = PartitionBySpu::default();
            new_group.followers.insert(partition);
            self.insert(spu, new_group);
        }
    }
    */
}

// used for selecting weight
pub(crate) enum SpuWeightSelection {
    Leader,
    Follower,
}

#[derive(Debug, Default)]
pub(crate) struct PartitionCountSpu {
    leaders: u16,
    followers: u16,
}

impl PartitionCountSpu {
    /// some fuzzy value to determine if how much this weight for replica
    /// less is more suitable for scheduling
    pub(crate) fn leader_weight(&self) -> u16 {
        self.leaders
    }

    pub(crate) fn follower_weight(&self) -> u16 {
        self.followers
    }
}

#[async_trait]
impl<C> PartitionLocalStorePolicy<C> for PartitionLocalStore<C>
where
    C: MetadataItem + Send + Sync,
{
    async fn names(&self) -> Vec<ReplicaKey> {
        self.read().await.keys().cloned().collect()
    }

    async fn topic_partitions(&self, topic: &str) -> Vec<PartitionMetadata<C>> {
        let mut res: Vec<PartitionMetadata<C>> = Vec::default();
        for (name, partition) in self.read().await.iter() {
            if name.topic == topic {
                res.push(partition.inner().clone());
            }
        }
        res
    }

    /// find all partitions that has spu in the replicas
    async fn partition_spec_for_spu(&self, target_spu: SpuId) -> Vec<(ReplicaKey, PartitionSpec)> {
        let mut res = vec![];
        for (name, partition) in self.read().await.iter() {
            if partition.spec.replicas.contains(&target_spu) {
                res.push((name.clone(), partition.spec.clone()));
            }
        }
        res
    }

    async fn count_topic_partitions(&self, topic: &str) -> i32 {
        let mut count: i32 = 0;
        for (name, _) in self.read().await.iter() {
            if name.topic == topic {
                count += 1;
            }
        }
        count
    }

    // return partitions that belong to this topic
    async fn topic_partitions_list(&self, topic: &str) -> Vec<ReplicaKey> {
        self.read()
            .await
            .keys()
            .filter_map(|name| {
                if name.topic == topic {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    async fn table_fmt(&self) -> String {
        let mut table = String::new();

        let partition_hdr = format!(
            "{n:<18}   {l:<6}  {r}\n",
            n = "PARTITION",
            l = "LEADER",
            r = "LIVE-REPLICAS",
        );
        table.push_str(&partition_hdr);

        for (name, partition) in self.read().await.iter() {
            let mut leader = String::from("-");
            let mut _lrs = String::from("[]");

            if partition.spec.leader >= 0 {
                leader = format!("{}", partition.spec.leader);
                //   lrs = partition.live_replicas_str();
            }
            let row = format!("{n:<18} {l:<6} \n", n = name.to_string(), l = leader,);
            table.push_str(&row);
        }

        table
    }

    /// replica msg for target spu
    async fn replica_for_spu(&self, target_spu: SpuId) -> Vec<Replica> {
        let msgs: Vec<Replica> = self
            .partition_spec_for_spu(target_spu)
            .await
            .into_iter()
            .map(|(replica_key, partition_spec)| {
                Replica::new(replica_key, partition_spec.leader, partition_spec.replicas)
            })
            .collect();
        debug!(
            "{} computing replica msg for spu y: {}, msg: {}",
            self,
            target_spu,
            msgs.len()
        );
        msgs
    }

    async fn leaders(&self) -> Vec<ReplicaLeader> {
        self.read()
            .await
            .iter()
            .map(|(key, value)| ReplicaLeader {
                id: key.clone(),
                leader: value.spec.leader,
            })
            .collect()
    }

    fn bulk_load(partitions: Vec<((impl Into<String>, PartitionId), Vec<SpuId>)>) -> Self {
        let elements = partitions
            .into_iter()
            .map(|(replica_key, replicas)| PartitionMetadata::quick((replica_key, replicas)))
            .collect();
        Self::bulk_new(elements)
    }

    /// count leades and follower by spu, this is used for scheduling
    async fn group_by_spu(&self) -> ReplicaSchedulingGroups {
        let mut groups = ReplicaSchedulingGroups::default();
        for (_, partition) in self.read().await.iter() {
            let leader = partition.spec.leader;
            groups.increase_leaders(leader);

            for follower_spu in partition.spec.replicas.iter() {
                if follower_spu != &leader {
                    groups.increase_followers(*follower_spu);
                }
            }
        }

        groups
    }
}

/// find status matching it,
fn find_status(status: &mut [ReplicaStatus], spu: SpuId) -> Option<&'_ mut ReplicaStatus> {
    status.iter_mut().find(|status| status.spu == spu)
}

pub(crate) trait PartitonStatusExtension: Sized {
    fn candidate_leader<P>(&self, online: &HashSet<SpuId>, policy: &P) -> Option<SpuId>
    where
        P: ElectionPolicy;

    fn merge(&mut self, other: Self);

    fn update_lrs(&mut self);
}

impl PartitonStatusExtension for PartitionStatus {
    fn candidate_leader<P>(&self, online: &HashSet<SpuId>, policy: &P) -> Option<SpuId>
    where
        P: ElectionPolicy,
    {
        let mut candidate_spu = None;
        let mut best_score = 0;

        for candidate in &self.replicas {
            // only do for live replicas
            if online.contains(&candidate.spu) {
                if let ElectionScoring::Score(score) =
                    policy.potential_leader_score(candidate, &self.leader)
                {
                    if candidate_spu.is_some() {
                        if score < best_score {
                            best_score = score;
                            candidate_spu = Some(candidate.spu);
                        }
                    } else {
                        best_score = score;
                        candidate_spu = Some(candidate.spu);
                    }
                }
            }
        }
        candidate_spu
    }

    /// merge status from spu
    /// ignore changes from spu = -1 or offsets = -1
    fn merge(&mut self, other: Self) {
        self.resolution = other.resolution;
        self.size = other.size;
        if let Some(old) = self.leader.merge(&other.leader) {
            self.replicas.push(old); // move old leader to replicas
        }

        for status in other.replicas {
            if let Some(old_status) = find_status(&mut self.replicas, status.spu) {
                old_status.merge(&status);
            } else {
                self.replicas.push(status);
            }
        }
        // delete any old status for leader in the follower
        let spu = self.leader.spu;
        self.replicas = self
            .replicas
            .iter()
            .filter_map(move |s| {
                if s.spu != spu {
                    Some(s.to_owned())
                } else {
                    None
                }
            })
            .collect();
        self.update_lrs();
    }

    /// recalculate lrs which is count of follower whose leo is same as leader
    fn update_lrs(&mut self) {
        let leader_leo = self.leader.leo;
        self.lsr = self
            .replicas
            .iter()
            .filter(|re| re.leo != -1 && leader_leo == re.leo)
            .count() as u32;
    }
}

pub(crate) trait ReplicaStatusExtension: Sized {
    /// merge status
    fn merge(&mut self, source: &Self) -> Option<Self>;
}

impl ReplicaStatusExtension for ReplicaStatus {
    /// merge status
    fn merge(&mut self, source: &Self) -> Option<Self> {
        // if source spu is -1, we ignore it
        if source.spu == -1 {
            return None;
        }

        // if spu is same, we override otherwise, we copy but return old
        if self.spu == -1 || self.spu == source.spu {
            self.spu = source.spu;

            if source.leo != -1 {
                self.leo = source.leo;
            }

            if source.hw != -1 {
                self.hw = source.hw;
            }
            None
        } else {
            let old = Self::new(self.spu, self.hw, self.leo);

            self.spu = source.spu;

            self.leo = source.leo;
            self.hw = source.hw;

            Some(old)
        }
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashSet;

    use crate::stores::partition::PartitonStatusExtension;

    use super::PartitionStatus;
    use super::ReplicaStatus;
    use super::ElectionPolicy;
    use super::ElectionScoring;

    struct SimplePolicy {}

    impl ElectionPolicy for SimplePolicy {
        fn potential_leader_score(
            &self,
            replica_status: &ReplicaStatus,
            leader: &ReplicaStatus,
        ) -> ElectionScoring {
            let lag = leader.leo - replica_status.leo;
            if lag < 4 {
                ElectionScoring::Score(lag as u16)
            } else {
                ElectionScoring::NotSuitable
            }
        }
    }

    #[test]
    fn test_candidate_spu_no_candidate() {
        let status = PartitionStatus::leader((5000, 0, 0));
        let online_spu = HashSet::new();
        let policy = SimplePolicy {};

        assert!(status.candidate_leader(&online_spu, &policy).is_none());
    }

    #[test]
    fn test_candidate_spu_best() {
        let status = PartitionStatus::new(
            (5000, 100, 110),
            vec![
                (5001, 100, 110).into(), // caught up with leader  (best)
                (5002, 100, 105).into(), // need 5 offset to caught with leaser
            ],
        );
        let mut online_spu = HashSet::new();
        online_spu.insert(5001);
        online_spu.insert(5002);
        let policy = SimplePolicy {};

        assert_eq!(status.candidate_leader(&online_spu, &policy), Some(5001)); // 5001 has least lag
    }

    /// we only care about which has least lag of end offset
    /// even if follower didn't catch up HW
    #[test]
    fn test_candidate_spu_best_conflict() {
        let status = PartitionStatus::new(
            (5000, 100, 110),
            vec![
                (5001, 95, 110).into(),  // caught up with leader  (best)
                (5002, 100, 105).into(), // need 5 offset to caught with leaser
            ],
        );

        let mut online_spu = HashSet::new();
        online_spu.insert(5000);
        online_spu.insert(5001);
        online_spu.insert(5002);
        let policy = SimplePolicy {};

        assert_eq!(status.candidate_leader(&online_spu, &policy), Some(5001)); // 5001 has least lag
    }

    /// check when we don't have any online
    #[test]
    fn test_candidate_spu_no_online() {
        let status = PartitionStatus::new(
            (5000, 100, 110),
            vec![
                (5001, 95, 110).into(),  // caught up with leader  (best)
                (5002, 100, 105).into(), // need 5 offset to caught with leaser
            ],
        );

        let online_spu = HashSet::new();
        let policy = SimplePolicy {};

        assert!(status.candidate_leader(&online_spu, &policy).is_none());
    }

    #[test]
    fn test_merge_initial() {
        let mut target = PartitionStatus::default();
        let source = PartitionStatus::leader((5000, 10, 11));
        target.merge(source);
        assert_eq!(target.leader, (5000, 10, 11).into());
        assert_eq!(target.replicas.len(), 0);

        let source = PartitionStatus::new((5000, 10, 11), vec![(5001, 9, 11).into()]);
        target.merge(source);

        assert_eq!(target.replicas.len(), 1);
        assert_eq!(target.replicas[0], (5001, 9, 11).into());
    }

    #[test]
    fn test_merge_lrs_full() {
        let mut target = PartitionStatus::new(
            (5000, 100, 110),
            vec![(5001, 95, 110).into(), (5002, 100, 105).into()],
        );

        let source = PartitionStatus::new(
            (5000, 120, 120),
            vec![(5002, 110, 120).into(), (5001, -1, -1).into()],
        );

        target.merge(source);

        assert_eq!(target.leader, (5000, 120, 120).into());
        assert_eq!(target.replicas[0], (5001, 95, 110).into());
        assert_eq!(target.replicas[1], (5002, 110, 120).into());
    }

    #[test]
    fn test_merge_lrs_different_leader() {
        let mut target = PartitionStatus::new((5000, 100, 110), vec![(5001, 95, 110).into()]);

        let source = PartitionStatus::new((5001, 120, 120), vec![(5000, -1, -1).into()]);

        target.merge(source);

        assert_eq!(target.leader, (5001, 120, 120).into());
        assert_eq!(target.replicas.len(), 1);
        assert_eq!(target.replicas[0], (5000, 100, 110).into());
    }

    #[test]
    fn test_merge_lrs_case_2() {
        let mut target =
            PartitionStatus::new((5002, 0, 0), vec![(5002, 0, 0).into(), (5001, 0, 0).into()]);

        let source = PartitionStatus::new((5002, 0, 0), vec![(5001, -1, -1).into()]);

        target.merge(source);

        assert_eq!(target.leader, (5002, 0, 0).into());
        assert_eq!(target.replicas.len(), 1);
        assert_eq!(target.replicas[0], (5001, 0, 0).into());
    }
}

#[cfg(test)]
mod test2 {

    use super::*;

    #[fluvio_future::test]
    async fn test_partitions_to_replica_msgs() {
        let partitions = DefaultPartitionStore::bulk_load(vec![(("topic1", 0), vec![10, 11, 12])]);
        let replica_msg = partitions.replica_for_spu(10).await;
        assert_eq!(replica_msg.len(), 1);
    }

    #[fluvio_future::test]
    async fn test_replica_group() {
        let partitions = DefaultPartitionStore::bulk_load(vec![
            (("t1", 0), vec![0]), // (topic1,0) at SPU 0
            (("t1", 1), vec![1]),
            (("t1", 2), vec![2]),
            (("t2", 0), vec![0, 3, 4]),
            (("t2", 1), vec![2, 4, 5]),
            (("t2", 2), vec![4, 5, 0]),
        ]);

        let groups = partitions.group_by_spu().await;
        println!("groups: {:#?}", groups);
        assert_eq!(groups.len(), 6); // spus are 0,1,2,3,4,5
        assert_eq!(groups[&0].leaders, 2); // spu 0 is leader for 1 partition
        assert_eq!(groups[&0].followers, 1); // spu 0 is follower for 1 partition
        assert_eq!(groups[&4].leaders, 1);
        assert_eq!(groups[&4].followers, 2);
    }

    #[test]
    fn test_spu_scheduling_simple() {
        let group = ReplicaSchedulingGroups::default();
        assert_eq!(
            group.find_suitable_spu(&vec![0, 1, 2], &vec![], SpuWeightSelection::Leader),
            Some(0)
        );

        assert_eq!(
            group.find_suitable_spu(&vec![0, 1, 2], &vec![0], SpuWeightSelection::Leader),
            Some(1)
        ); // anti-affinity
        assert_eq!(
            group.find_suitable_spu(&vec![0, 1, 2], &vec![0, 1], SpuWeightSelection::Leader),
            Some(2)
        ); // anti-affinity

        assert_eq!(
            group.find_suitable_spu(&vec![1, 2], &vec![], SpuWeightSelection::Follower),
            Some(1)
        );

        //println!("partitions: {:#?}", group);
    }

    #[fluvio_future::test]
    async fn test_spu_scheduling_no_empty() {
        let partitions = DefaultPartitionStore::bulk_load(vec![
            (("t1", 0), vec![0]),
            (("t1", 1), vec![1, 6]),
            (("t1", 2), vec![2, 4, 5]),
            (("t2", 0), vec![0, 3]),
            (("t2", 1), vec![2, 4]),
            (("t2", 2), vec![5, 0]),
        ]);

        let groups = partitions.group_by_spu().await;
        //println!("groups: {:#?}", groups);
        // this yields
        // 0 -> leaders: 2, followers: 1
        // 1 -> leaders: 1, followers: 0
        // 2 -> leaders: 2, followers: 0
        // 3 -> leaders: 0, followers: 1
        // 4 -> leaders: 0, followers: 2
        // 5 -> leaders: 1, followers: 1
        // 6 -> leaders: 0, followers: 1

        assert_eq!(
            groups.find_suitable_spu(&vec![0, 1, 2], &vec![], SpuWeightSelection::Leader),
            Some(1)
        );
        assert_eq!(
            groups.find_suitable_spu(&vec![0, 1, 2], &vec![2], SpuWeightSelection::Leader),
            Some(1)
        ); // anti-affinity
        assert_eq!(
            groups.find_suitable_spu(&vec![1, 2, 3, 6], &vec![], SpuWeightSelection::Leader),
            Some(3)
        ); // anti-affinity

        assert_eq!(
            groups.find_suitable_spu(&vec![2, 3, 6], &vec![], SpuWeightSelection::Follower),
            Some(2)
        ); // anti-affinity
    }
}
