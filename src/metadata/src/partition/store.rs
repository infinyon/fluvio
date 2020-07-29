//!
//! # Partition & Partitions Metadata
//!
//! Partition metadata information on cached in the local Controller.
//!

use std::sync::Arc;
use std::ops::Deref;

use log::debug;

use flv_types::SpuId;

use crate::store::*;
use crate::core::*;
use super::*;

pub type SharedPartitionStore<C> = Arc<PartitionLocalStore<C>>;


pub type DefaultPartitionMd = PartitionMetadata<String>;
pub type DefaultPartitionStore = PartitionLocalStore<String>;

pub struct PartitionMetadata<C: MetadataItem>(MetadataStoreObject<PartitionSpec, C>);

impl<C: MetadataItem> Deref for PartitionMetadata<C> {
    type Target = MetadataStoreObject<PartitionSpec, C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl <C: MetadataItem> From<MetadataStoreObject<PartitionSpec,C>> for PartitionMetadata<C> {
    fn from(partition: MetadataStoreObject<PartitionSpec,C>) -> Self {
        Self(partition)
    }
}

impl <C: MetadataItem> Into<MetadataStoreObject<PartitionSpec,C>> for PartitionMetadata<C> {
    fn into(self) -> MetadataStoreObject<PartitionSpec,C> {
        self.0
    }
}



impl<C> PartitionMetadata<C>
where
    C: MetadataItem,
{
    /// create new partition with replica map.
    /// first element of replicas is leader
    pub fn with_replicas(key: ReplicaKey, replicas: Vec<SpuId>) -> Self {
        let spec: PartitionSpec = replicas.into();
        Self(MetadataStoreObject::new(key, spec, PartitionStatus::default()))
    }
}

impl<S, C> From<((S, i32), Vec<i32>)> for PartitionMetadata<C>
where
    S: Into<String>,
    C: MetadataItem,
{
    fn from(partition: ((S, i32), Vec<i32>)) -> Self {
        let (replica_key, replicas) = partition;
        Self::with_replicas(replica_key.into(), replicas)
    }
}


pub struct PartitionLocalStore<C: MetadataItem>(LocalStore<PartitionSpec, C>);

impl<C: MetadataItem> Deref for PartitionLocalStore<C> {
    type Target = LocalStore<PartitionSpec, C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


impl<C> PartitionLocalStore<C>
where
    C: MetadataItem,
{
    pub async fn names(&self) -> Vec<ReplicaKey> {
        self.read().await.keys().cloned().collect()
    }

    pub async fn topic_partitions(&self, topic: &str) -> Vec<PartitionMetadata<C>> {
        let mut res: Vec<PartitionMetadata<C>> = Vec::default();
        for (name, partition) in self.read().await.iter() {
            if name.topic == topic {
                res.push(partition.inner().clone().into());
            }
        }
        res
    }

    /// find all partitions that has spu in the replicas
    pub async fn partition_spec_for_spu(
        &self,
        target_spu: i32,
    ) -> Vec<(ReplicaKey, PartitionSpec)> {
        let mut res = vec![];
        for (name, partition) in self.read().await.iter() {
            if partition.spec.replicas.contains(&target_spu) {
                res.push((name.clone(), partition.spec.clone()));
            }
        }
        res
    }

    pub async fn count_topic_partitions(&self, topic: &str) -> i32 {
        let mut count: i32 = 0;
        for (name, _) in self.read().await.iter() {
            if name.topic == topic {
                count += 1;
            }
        }
        count
    }

    // return partitions that belong to this topic
    #[allow(dead_code)]
    async fn topic_partitions_list(&self, topic: &str) -> Vec<ReplicaKey> {
        self.read()
            .await
            .keys()
            .filter_map(|name| {
                if &name.topic == topic {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub async fn table_fmt(&self) -> String {
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
    pub async fn replica_for_spu(&self, target_spu: SpuId) -> Vec<Replica> {
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
            self.0,
            target_spu,
            msgs.len()
        );
        msgs
    }

    pub async fn leaders(&self) -> Vec<ReplicaLeader> {
        self.read()
            .await
            .iter()
            .map(|(key, value)| ReplicaLeader {
                id: key.clone(),
                leader: value.spec.leader,
            })
            .collect()
    }
}

impl<C, S> From<Vec<((S, i32), Vec<i32>)>> for PartitionLocalStore<C>
where
    S: Into<String>,
    C: MetadataItem,
{
    fn from(partitions: Vec<((S, i32), Vec<i32>)>) -> Self {
        let elements = partitions
            .into_iter()
            .map(|(replica_key, replicas)| {
                let p: PartitionMetadata<C> = (replica_key, replicas).into();
                p.0
            })
            .collect();
        Self(LocalStore::bulk_new(elements))
    }
}

#[cfg(test)]
pub mod test {

    use flv_future_aio::test_async;
    use super::*;

    #[test_async]
    async fn test_partitions_to_replica_msgs() -> Result<(), ()> {
        let partitions: DefaultPartitionStore = vec![(("topic1", 0), vec![10, 11, 12])].into();
        let replica_msg = partitions.replica_for_spu(10).await;
        assert_eq!(replica_msg.len(), 1);
        Ok(())
    }
}
