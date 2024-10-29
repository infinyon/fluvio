use fluvio_controlplane::PartitionMetadata;
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_protocol::record::ReplicaKey;
use fluvio_stream_model::{
    store::{MetadataStoreObject, LocalStore},
    core::MetadataItem,
};
use tracing::{debug, trace};
use async_trait::async_trait;

use crate::stores::partition::PartitionLocalStore;

use super::*;

pub type TopicMetadata<C> = MetadataStoreObject<TopicSpec, C>;
pub type TopicLocalStore<C> = LocalStore<TopicSpec, C>;
pub type DefaultTopicMd = TopicMetadata<u32>;
pub type DefaultTopicLocalStore = TopicLocalStore<u32>;

#[async_trait]
pub trait TopicMd<C: MetadataItem> {
    async fn create_new_partitions(
        &self,
        partition_store: &PartitionLocalStore<C>,
    ) -> Vec<PartitionMetadata<C>>;
}

#[async_trait]
impl<C: MetadataItem> TopicMd<C> for TopicMetadata<C>
where
    C: MetadataItem + Send + Sync,
{
    /// create new partitions from the replica map if it doesn't exists
    async fn create_new_partitions(
        &self,
        partition_store: &PartitionLocalStore<C>,
    ) -> Vec<PartitionMetadata<C>> {
        let mut partitions = vec![];
        let replica_map = &self.status.replica_map;
        trace!(?replica_map, "creating new partitions for topic");
        let store = partition_store.read().await;
        for (idx, replicas) in replica_map.iter() {
            let mirror = self.status.mirror_map.get(idx);

            let replica_key = ReplicaKey::new(self.key(), *idx);

            let partition_spec = PartitionSpec::from_replicas(replicas.clone(), &self.spec, mirror);
            if !store.contains_key(&replica_key) {
                debug!(?replica_key, ?partition_spec, "creating new partition");
                partitions.push(
                    MetadataStoreObject::with_spec(replica_key, partition_spec)
                        .with_context(self.ctx.create_child()),
                )
            } else {
                debug!(?replica_key, "partition already exists");
            }
        }
        drop(store);
        partitions
    }
}

#[async_trait]
pub trait TopicLocalStorePolicy<C>
where
    C: MetadataItem,
{
    async fn table_fmt(&self) -> String;
}

#[async_trait]
impl<C> TopicLocalStorePolicy<C> for TopicLocalStore<C>
where
    C: MetadataItem + Send + Sync,
{
    async fn table_fmt(&self) -> String {
        let mut table = String::new();

        let topic_hdr = format!(
            "{n:<18}   {t:<8}  {p:<5}  {s:<5}  {g:<8}  {l:<14}  {m:<10}  {r}\n",
            n = "TOPIC",
            t = "TYPE",
            p = "PART",
            s = "FACT",
            g = "IGN-RACK",
            l = "RESOLUTION",
            m = "R-MAP-ROWS",
            r = "REASON",
        );
        table.push_str(&topic_hdr);

        for (name, topic) in self.read().await.iter() {
            let topic_row = format!(
                "{n:<18}  {t:^8}  {p:^5}  {s:^5}  {g:<8}  {l:^14}  {m:^10}  {r}\n",
                n = name.clone(),
                t = topic.spec.type_label(),
                p = topic.spec.partitions_display(),
                s = topic.spec.replication_factor_display(),
                g = topic.spec.ignore_rack_assign_display(),
                l = topic.status.resolution().resolution_label(),
                m = topic.status.replica_map_cnt_str(),
                r = topic.status.reason
            );
            table.push_str(&topic_row);
        }

        table
    }
}

#[cfg(test)]
mod test {
    use fluvio_controlplane_metadata::topic::{TopicStatus, TopicResolution};
    use fluvio_protocol::record::ReplicaKey;
    use fluvio_sc_schema::{
        partition::{PartitionSpec, PartitionStatus},
        store::MetadataStoreObject,
        topic::TopicSpec,
    };

    use crate::stores::{
        partition::DefaultPartitionStore,
        topic::{DefaultTopicLocalStore, DefaultTopicMd, TopicMd},
    };

    #[test]
    fn test_topic_replica_map() {
        // empty replica map
        let topic1 = DefaultTopicMd::new("Topic-1", (1, 1, false).into(), TopicStatus::default());
        assert_eq!(topic1.status.replica_map.len(), 0);

        // replica map with 2 partitions
        let topic2 = DefaultTopicMd::new(
            "Topic-2",
            (1, 1, false).into(),
            TopicStatus::new(
                TopicResolution::Provisioned,
                vec![vec![0, 1], vec![1, 2]],
                "".to_owned(),
            ),
        );
        assert_eq!(topic2.status.replica_map.len(), 2);
    }

    #[test]
    fn test_update_topic_status_objects() {
        // create topic 1
        let mut topic1 =
            DefaultTopicMd::new("Topic-1", (2, 2, false).into(), TopicStatus::default());
        assert_eq!(topic1.status.resolution, TopicResolution::Init);

        // create topic 2
        let topic2 = DefaultTopicMd::new(
            "Topic-1",
            (2, 2, false).into(),
            TopicStatus::new(
                TopicResolution::Provisioned,
                vec![vec![0, 1], vec![1, 2]],
                "".to_owned(),
            ),
        );

        // test update individual components
        topic1
            .status
            .set_replica_map(topic2.status.replica_map.clone());
        topic1.status.reason.clone_from(&topic2.status.reason);
        topic1.status.resolution = topic2.status.resolution.clone();

        // topics should be identical
        assert_eq!(topic1, topic2);
    }

    #[fluvio_future::test]
    async fn test_topics_in_pending_state() {
        use std::collections::HashSet;

        // resolution: Init
        let topic1 = DefaultTopicMd::new("Topic-1", (1, 1, false).into(), TopicStatus::default());
        assert!(topic1.status.is_resolution_initializing());

        // resolution: Pending
        let topic2 = DefaultTopicMd::new(
            "Topic-2",
            (1, 1, false).into(),
            TopicStatus::new(
                TopicResolution::Pending,
                vec![],
                "waiting for live spus".to_owned(),
            ),
        );
        assert!(topic2.status.is_resolution_pending());

        // resolution: Ok
        let topic3 = DefaultTopicMd::new(
            "Topic-3",
            (2, 2, false).into(),
            TopicStatus::new(
                TopicResolution::Provisioned,
                vec![vec![0, 1], vec![1, 2]],
                "".to_owned(),
            ),
        );
        assert!(topic3.status.is_resolution_provisioned());

        // resolution: Inconsistent
        let topic4 = DefaultTopicMd::new(
            "Topic-4",
            (2, 2, false).into(),
            TopicStatus::new(
                TopicResolution::InsufficientResources,
                vec![vec![0], vec![1]],
                "".to_owned(),
            ),
        );

        let topics = DefaultTopicLocalStore::bulk_new(vec![topic1, topic2, topic3, topic4]);

        let expected: HashSet<String> = vec![String::from("Topic-2"), String::from("Topic-4")]
            .into_iter()
            .collect();
        let mut pending_state_names: HashSet<String> = HashSet::new();

        for topic in topics.read().await.values() {
            if topic.status.need_replica_map_recal() {
                pending_state_names.insert(topic.key_owned());
            }
        }

        assert_eq!(pending_state_names, expected);
    }

    #[fluvio_future::test]
    async fn test_partitions_from_replicas() {
        // already exist a partition with spu leader 0 and replica 1
        let partition_stored = MetadataStoreObject::<PartitionSpec, u32>::new(
            ReplicaKey::new("topic-1", 0_u32),
            PartitionSpec::new(0, vec![0, 1]),
            PartitionStatus::default(),
        );

        // create topic with 2 partitions (but already exist one)
        let spec: TopicSpec = (2, 2, false).into();
        let key = "topic-1";
        let status = TopicStatus::new(
            TopicResolution::Provisioned,
            vec![vec![0, 1], vec![1, 2]],
            "".to_owned(),
        );
        let topic = MetadataStoreObject::<TopicSpec, u32>::new(key, spec, status);
        let partition_store = DefaultPartitionStore::bulk_new(vec![partition_stored]);

        let partitions = topic.create_new_partitions(&partition_store).await;

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].key, ReplicaKey::new("topic-1", 1_u32));
        assert_eq!(partitions[0].spec.leader, 1);
    }
}
