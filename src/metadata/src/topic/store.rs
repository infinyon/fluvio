use std::ops::Deref;
use std::ops::DerefMut;

use log::debug;


use crate::store::*;
use crate::core::*;
use crate::partition::store::*;
use crate::partition::*;
use super::*;


pub type DefaultTopicMd = TopicMetadata<String>;
pub type DefaultTopicLocalStore = TopicLocalStore<String>;

#[derive(PartialEq,Debug)]
pub struct TopicMetadata<C: MetadataItem>(MetadataStoreObject<TopicSpec, C>);

impl<C: MetadataItem> Deref for TopicMetadata<C> {
    type Target = MetadataStoreObject<TopicSpec, C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl <C: MetadataItem> DerefMut for TopicMetadata<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


impl <C: MetadataItem> From<MetadataStoreObject<TopicSpec,C>> for TopicMetadata<C> {
    fn from(spu: MetadataStoreObject<TopicSpec,C>) -> Self {
        Self(spu)
    }
}

impl <C: MetadataItem> Into<MetadataStoreObject<TopicSpec,C>> for TopicMetadata<C> {
    fn into(self) -> MetadataStoreObject<TopicSpec,C> {
        self.0
    }
}



impl<C> std::fmt::Display for TopicMetadata<C>
where
    C: MetadataItem,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.spec {
            TopicSpec::Assigned(partition_map) => write!(f, "assigned::{}", partition_map),
            TopicSpec::Computed(param) => write!(f, "computed::({})", param),
        }
    }
}

impl<C> TopicMetadata<C>
where
    C: MetadataItem,
{


    /// create new partitions from my replica map if it doesn't exists
    /// from partition store
    pub async fn create_new_partitions(
        &self,
        partition_store: &PartitionLocalStore<C>,
    ) -> Vec<PartitionMetadata<C>> {
        let mut partitions = vec![];
        for (idx, replicas) in self.status.replica_map.iter() {
            let replica_key = ReplicaKey::new(self.key(), *idx);
            debug!("Topic: {} creating partition: {}", self.key(), replica_key);
            if !partition_store.contains_key(&replica_key).await {
                partitions.push(
                    MetadataStoreObject::with_spec(replica_key, replicas.clone().into())
                        .with_context(self.ctx.create_child()).into(),
                )
            }
        }
        partitions
    }
}

pub struct TopicLocalStore<C: MetadataItem>(LocalStore<TopicSpec, C>);

impl <C: MetadataItem> From<LocalStore<TopicSpec, C>> for TopicLocalStore<C> {
    fn from(store: LocalStore<TopicSpec, C>) -> Self {
        Self(store)
    }
}



impl<C: MetadataItem> Deref for TopicLocalStore<C> {
    type Target = LocalStore<TopicSpec, C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}




impl<C> TopicLocalStore<C>
where
    C: MetadataItem,
{
    pub async fn topic(&self, topic_name: &str) -> Option<TopicMetadata<C>> {
        match self.read().await.get(topic_name) {
            Some(topic) => Some(topic.inner().clone().into()),
            None => None,
        }
    }

    pub async fn table_fmt(&self) -> String {
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

    use flv_future_aio::test_async;
    use crate::topic::store::DefaultTopicMd;
    use crate::topic::TopicStatus;
    use crate::topic::TopicResolution;
    use crate::topic::store::DefaultTopicLocalStore;
    use crate::store::LocalStore;
    use crate::store::MetadataStoreObject;

    #[test]
    fn test_topic_replica_map() {
        // empty replica map
        let topic1: DefaultTopicMd = MetadataStoreObject::new("Topic-1", (1, 1, false).into(), TopicStatus::default()).into();
        assert_eq!(topic1.status.replica_map.len(), 0);

        // replica map with 2 partitions
        let topic2: DefaultTopicMd = MetadataStoreObject::new(
            "Topic-2",
            (1, 1, false).into(),
            TopicStatus::new(
                TopicResolution::Provisioned,
                vec![vec![0, 1], vec![1, 2]],
                "".to_owned(),
            ),
        ).into();
        assert_eq!(topic2.status.replica_map.len(), 2);
    }

    #[test]
    fn test_update_topic_status_objects() {
        // create topic 1
        let mut topic1: DefaultTopicMd = MetadataStoreObject::new("Topic-1", (2, 2, false).into(), TopicStatus::default()).into();
        assert_eq!(topic1.status.resolution, TopicResolution::Init);

        // create topic 2
        let topic2: DefaultTopicMd = MetadataStoreObject::new(
            "Topic-1",
            (2, 2, false).into(),
            TopicStatus::new(
                TopicResolution::Provisioned,
                vec![vec![0, 1], vec![1, 2]],
                "".to_owned(),
            ),
        ).into();

        // test update individual components
        topic1.status.set_replica_map(topic2.status.replica_map.clone());
        topic1.status.reason = topic2.status.reason.clone();
        topic1.status.resolution = (&topic2.status.resolution).clone();

        // topics should be identical
        assert_eq!(topic1, topic2);
    }

    #[test_async]
    async fn test_topics_in_pending_state() -> Result<(), ()> {
        use std::collections::HashSet;

        // resolution: Init
        let topic1: DefaultTopicMd = MetadataStoreObject::new("Topic-1", (1, 1, false).into(), TopicStatus::default()).into();
        assert_eq!(topic1.status.is_resolution_initializing(), true);

        // resolution: Pending
        let topic2: DefaultTopicMd = MetadataStoreObject::new(
            "Topic-2",
            (1, 1, false).into(),
            TopicStatus::new(
                TopicResolution::Pending,
                vec![],
                "waiting for live spus".to_owned(),
            ),
        ).into();
        assert_eq!(topic2.status.is_resolution_pending(), true);

        // resolution: Ok
        let topic3: DefaultTopicMd = MetadataStoreObject::new(
            "Topic-3",
            (2, 2, false).into(),
            TopicStatus::new(
                TopicResolution::Provisioned,
                vec![vec![0, 1], vec![1, 2]],
                "".to_owned(),
            ),
        ).into();
        assert_eq!(topic3.status.is_resolution_provisioned(), true);

        // resolution: Inconsistent
        let topic4: DefaultTopicMd = MetadataStoreObject::new(
            "Topic-4",
            (2, 2, false).into(),
            TopicStatus::new(
                TopicResolution::InsufficientResources,
                vec![vec![0], vec![1]],
                "".to_owned(),
            ),
        ).into();

        let topics: DefaultTopicLocalStore = LocalStore::bulk_new(vec![topic1, topic2, topic3, topic4]).into();

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
        Ok(())
    }
}



#[cfg(test)]
pub mod test2 {

    use flv_future_aio::test_async;

    
    use crate::store::actions::LSUpdate;
    use crate::store::DefaultMetadataObject;
    use crate::store::LocalStore;

    use super::TopicSpec;
    use super::TopicStatus;
    use super::TopicResolution;

    type DefaultTopic = DefaultMetadataObject<TopicSpec>;
    type DefaultTopicStore = LocalStore<TopicSpec,String>;

    #[test_async]
    async fn test_store_check_items_against_empty() -> Result<(), ()> {
        let topics = vec![DefaultTopic::with_spec("topic1", TopicSpec::default())];
        let topic_store = DefaultTopicStore::default();
        assert_eq!(topic_store.epoch().await, 0);

        let status = topic_store.sync_all(topics).await;
        assert_eq!(topic_store.epoch().await, 1);
        assert_eq!(status.add, 1);
        assert_eq!(status.delete, 0);
        assert_eq!(status.update, 0);

        let read_guard = topic_store.read().await;
        let topic1 = read_guard.get("topic1").expect("topic1 should exists");
        assert_eq!(topic1.epoch(), 1);

        Ok(())
    }

    #[test_async]
    async fn test_store_check_items_against_same() -> Result<(), ()> {
        let topic_store = DefaultTopicStore::bulk_new(vec![DefaultTopic::with_spec(
            "topic1",
            TopicSpec::default(),
        )]);

        let status = topic_store
            .sync_all(vec![DefaultTopic::with_spec(
                "topic1",
                TopicSpec::default(),
            )])
            .await;
        assert_eq!(status.epoch, 1);

        Ok(())
    }

    #[test_async]
    async fn test_store_sync_all() -> Result<(), ()> {
        let topic1 = DefaultTopic::with_spec("topic1", TopicSpec::default());
        let topic2 = DefaultTopic::with_spec("topic2", TopicSpec::default());

        let topic_store = DefaultTopicStore::bulk_new(vec![topic1.clone(), topic2.clone()]);
        assert_eq!(topic_store.epoch().await, 0);

        // same input as existing items, so there should not be any changes
        let status = topic_store
            .sync_all(vec![topic1.clone(), topic2.clone()])
            .await;
        assert_eq!(status.epoch, 1);

        // do another sync, in this case there is only 1 topic1, so there should be 1 delete
        let status = topic_store.sync_all(vec![topic1.clone()]).await;
        assert_eq!(status.add, 0);
        assert_eq!(status.update, 0);
        assert_eq!(status.delete, 1);
        assert_eq!(status.epoch, 2);
        assert_eq!(topic_store.epoch().await, 2);

        Ok(())
    }

    #[test_async]
    async fn test_sync_all_update_status() -> Result<(), ()> {
        let old_topic = DefaultTopic::with_spec("topic1", TopicSpec::default());

        let topic_store = DefaultTopicStore::bulk_new(vec![old_topic]);
        assert_eq!(topic_store.epoch().await, 0);

        // same topic key but with different status
        let mut status = TopicStatus::default();
        status.resolution = TopicResolution::Provisioned;
        let updated_topic = DefaultTopic::new("topic1", TopicSpec::default(), status);

        let status = topic_store.sync_all(vec![updated_topic.clone()]).await;
        assert_eq!(status.add, 0);
        assert_eq!(status.update, 1);
        assert_eq!(status.delete, 0);
        assert_eq!(topic_store.epoch().await, 1);

        let read = topic_store.read().await;
        let topic = read.get("topic1").expect("get");
        assert_eq!(topic.inner(), &updated_topic);
        assert_eq!(topic.epoch(), 1);

        Ok(())
    }

    #[test_async]
    async fn test_store_items_delete() -> Result<(), ()> {
        let topic_store = DefaultTopicStore::bulk_new(vec![DefaultTopic::with_spec(
            "topic1",
            TopicSpec::default(),
        )]);

        let status = topic_store.sync_all(vec![]).await;

        assert_eq!(status.add, 0);
        assert_eq!(status.delete, 1);
        assert_eq!(status.update, 0);

        assert_eq!(topic_store.read().await.len(), 0); // there should be no more topics

        Ok(())
    }

    #[test_async]
    async fn test_store_apply_add_actions() -> Result<(), ()> {
        let new_topic = DefaultTopic::with_spec("topic1", TopicSpec::default());

        let changes = vec![LSUpdate::Mod(new_topic.clone())];

        let topic_store = DefaultTopicStore::default();

        let status = topic_store.apply_changes(changes).await.expect("some");

        assert_eq!(status.add, 1);
        assert_eq!(status.update, 0);
        assert_eq!(status.delete, 0);

        topic_store
            .read()
            .await
            .get("topic1")
            .expect("topic1 should exists");

        Ok(())
    }

    /// test end to end change tracking
    #[test_async]
    async fn test_changes() -> Result<(), ()> {
        let topic_store = DefaultTopicStore::default();
        assert_eq!(topic_store.epoch().await, 0);

        let topic1 = DefaultTopic::with_spec("topic1", TopicSpec::default());
        let topics = vec![topic1.clone()];
        let status = topic_store.sync_all(topics).await;
        assert_eq!(status.epoch, 1);

        {
            let read_guard = topic_store.read().await;
            // should return full list since we try to read prior fencing
            let changes = read_guard.changes_since(0);
            assert_eq!(changes.epoch, 1);
            assert_eq!(changes.is_sync_all(), true);
            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            // should return empty since we don't have new changes
            let changes = read_guard.changes_since(1);
            assert_eq!(changes.epoch, 1);
            assert_eq!(changes.is_sync_all(), false);
            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);
        }

        // add new topic
        let topic2 = DefaultTopic::with_spec("topic2", TopicSpec::default());
        let status = topic_store
            .apply_changes(vec![LSUpdate::Mod(topic2)])
            .await
            .expect("some");
        assert_eq!(status.epoch, 2);

        {
            let read_guard = topic_store.read().await;

            // read prior to fence always yield full list
            let changes = read_guard.changes_since(0);
            assert_eq!(changes.epoch, 2);
            assert_eq!(changes.is_sync_all(), true);
            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 2);
            assert_eq!(deletes.len(), 0);

            // list of change since epoch 2 w
            let changes = read_guard.changes_since(1);
            assert_eq!(changes.epoch, 2);
            assert_eq!(changes.is_sync_all(), false);
            let (updates, _) = changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(
                updates[0],
                DefaultTopic::with_spec("topic2", TopicSpec::default())
            );

            let changes = read_guard.changes_since(2);
            assert_eq!(changes.epoch, 2);
            let (updates, _) = changes.parts();
            assert_eq!(updates.len(), 0);
        }

        // perform delete
        let status = topic_store
            .apply_changes(vec![LSUpdate::Delete("topic1".to_owned())])
            .await
            .expect("some");
        assert_eq!(status.epoch, 3);

        {
            let read_guard = topic_store.read().await;
            let changes = read_guard.changes_since(1);
            assert_eq!(changes.epoch, 3);
            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 1);

            let changes = read_guard.changes_since(2);
            assert_eq!(changes.epoch, 3);
            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 1);
            assert_eq!(deletes[0], topic1);

            let changes = read_guard.changes_since(3);
            assert_eq!(changes.epoch, 3);
            assert_eq!(changes.is_sync_all(), false);
            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);
        }

        Ok(())
    }
}
