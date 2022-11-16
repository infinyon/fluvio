//!
//! # Partition & Partitions Metadata
//!
//! Partition metadata information on cached in the local Controller.
//!

use std::sync::Arc;

use fluvio_types::PartitionId;
use tracing::debug;
use async_trait::async_trait;

use fluvio_types::SpuId;

use crate::store::*;
use crate::core::*;
use super::*;

pub type SharedPartitionStore<C> = Arc<PartitionLocalStore<C>>;

pub type PartitionMetadata<C> = MetadataStoreObject<PartitionSpec, C>;
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

#[cfg(test)]
pub mod test {

    use super::*;

    #[fluvio_future::test]
    async fn test_partitions_to_replica_msgs() {
        let partitions = DefaultPartitionStore::bulk_load(vec![(("topic1", 0), vec![10, 11, 12])]);
        let replica_msg = partitions.replica_for_spu(10).await;
        assert_eq!(replica_msg.len(), 1);
    }
}
