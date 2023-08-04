//!
//! # Partition & Partitions Metadata
//!
//! Partition metadata information on cached in the local Controller.
//!

use std::collections::HashSet;
use std::sync::Arc;

use tracing::debug;
use async_trait::async_trait;

use fluvio_controlplane::replica::Replica;
use fluvio_controlplane::replica::ReplicaLeader;
use fluvio_stream_model::core::MetadataItem;

use fluvio_stream_model::store::LocalStore;
use fluvio_types::PartitionId;
use fluvio_types::SpuId;

use super::*;
use super::policy::ElectionPolicy;
use super::policy::ElectionScoring;

pub type SharedPartitionStore<C> = Arc<PartitionLocalStore<C>>;

pub type PartitionLocalStore<C> = LocalStore<PartitionSpec, C>;
pub type DefaultPartitionMd = PartitionMetadata<String>;
pub type DefaultPartitionStore = PartitionLocalStore<u32>;

pub trait PartitionMd<C: MetadataItem> {
    fn with_replicas(key: ReplicaKey, replicas: Vec<SpuId>) -> Self;

    fn quick<S: Into<String>>(partition: ((S, PartitionId), Vec<SpuId>)) -> Self;
}

impl<C: MetadataItem> PartitionMd<C> for PartitionMetadata<C> {
    /// create new partition with replica map.
    /// first element of replicas is leader
    fn with_replicas(key: ReplicaKey, replicas: Vec<SpuId>) -> Self {
        let spec: PartitionSpec = replicas.into();
        Self::new(key, spec, PartitionStatus::default())
    }

    fn quick<S: Into<String>>(partition: ((S, PartitionId), Vec<SpuId>)) -> Self {
        let (replica_key, replicas) = partition;
        Self::with_replicas(replica_key.into(), replicas)
    }
}

#[async_trait]
pub trait PartitionLocalStorePolicy<C>
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

    fn bulk_load<S: Into<String>>(partitions: Vec<((S, PartitionId), Vec<SpuId>)>) -> Self;
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

    fn bulk_load<S: Into<String>>(partitions: Vec<((S, PartitionId), Vec<SpuId>)>) -> Self {
        let elements = partitions
            .into_iter()
            .map(|(replica_key, replicas)| PartitionMetadata::quick((replica_key, replicas)))
            .collect();
        Self::bulk_new(elements)
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
}
