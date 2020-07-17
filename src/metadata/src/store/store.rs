use std::sync::Arc;
use std::fmt::Debug;
use std::fmt::Display;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use log::debug;
use log::error;
use flv_future_aio::sync::RwLock;
use flv_future_aio::sync::RwLockReadGuard;
use flv_future_aio::sync::RwLockWriteGuard;

use crate::core::*;
use super::actions::*;
use super::epoch_map::*;
use super::*;

pub enum CheckExist {
    // doesn't exist
    None,
    // exists, but same value
    Same,
    // exists, but different
    Different,
}

/// Idempotent local memory cache of meta objects.
/// There are only 2 write operations are permitted: sync and apply changes which are idempotent.
/// For read, read guards are provided which provide hash map API using deref.  
/// Hash values are wrapped in EpochCounter.  EpochCounter is also deref.
/// Using async lock to ensure read/write are thread safe.
#[derive(Debug)]
pub struct LocalStore<S, C>(RwLock<EpochMap<S::IndexKey, MetadataStoreObject<S, C>>>)
where
    S: Spec,
    C: MetadataItem;

impl<S, C> Default for LocalStore<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    fn default() -> Self {
        Self(RwLock::new(EpochMap::new()))
    }
}

impl<S, C> LocalStore<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    /// initialize local stores with list of metadata objects
    pub fn bulk_new(objects: Vec<MetadataStoreObject<S, C>>) -> Self {
        let mut map = HashMap::new();
        for obj in objects {
            map.insert(obj.key.clone(), obj.into());
        }
        Self(RwLock::new(EpochMap::new_with_map(map)))
    }

    /// create arc wrapper
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Read guard
    #[inline(always)]
    pub async fn read<'a>(
        &'a self,
    ) -> RwLockReadGuard<'a, EpochMap<S::IndexKey, MetadataStoreObject<S, C>>> {
        self.0.read().await
    }

    /// write guard, this is private, use sync API to make changes
    #[inline(always)]
    async fn write<'a>(
        &'a self,
    ) -> RwLockWriteGuard<'a, EpochMap<S::IndexKey, MetadataStoreObject<S, C>>> {
        self.0.write().await
    }

    /// current epoch
    pub async fn epoch(&self) -> i64 {
        self.read().await.epoch()
    }

    /// initial epoch that should be used
    /// store will always have epoch greater than init_epoch if there are any changes
    pub fn init_epoch(&self) -> EpochCounter<()> {
        EpochCounter::default()
    }

    /// copy of the value
    pub async fn value<K: ?Sized>(&self, key: &K) -> Option<EpochCounter<MetadataStoreObject<S, C>>>
    where
        S::IndexKey: Borrow<K>,
        K: Eq + Hash,
    {
        match self.read().await.get(key) {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }

    /// copy spec
    pub async fn spec<K: ?Sized>(&self, key: &K) -> Option<S>
    where
        S::IndexKey: Borrow<K>,
        K: Eq + Hash,
    {
        match self.read().await.get(key) {
            Some(value) => Some(value.spec.clone()),
            None => None,
        }
    }

    /// iterate over entry
    pub async fn find_and_do<K, F>(&self, key: &K, mut func: F) -> Option<()>
    where
        F: FnMut(&'_ MetadataStoreObject<S, C>),
        K: Eq + Hash,
        S::IndexKey: Borrow<K>,
    {
        if let Some(value) = self.read().await.get(key) {
            func(value);
            Some(())
        } else {
            None
        }
    }

    pub async fn contains_key<K: ?Sized>(&self, key: &K) -> bool
    where
        S::IndexKey: Borrow<K>,
        K: Eq + Hash,
    {
        self.read().await.contains_key(key)
    }

    pub async fn count(&self) -> i32 {
        self.read().await.len() as i32
    }

    pub async fn clone_specs(&self) -> Vec<S> {
        self.read()
            .await
            .values()
            .map(|kv| kv.spec.clone())
            .collect()
    }

    pub async fn clone_keys(&self) -> Vec<S::IndexKey> {
        self.read().await.clone_keys()
    }

    pub async fn clone_values(&self) -> Vec<MetadataStoreObject<S, C>> {
        self.read().await.clone_values()
    }
}

impl<S, C> Display for LocalStore<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use flv_future_aio::task::run_block_on;
        let len = run_block_on(async { self.read().await.len() });
        write!(f, "{} Store count: {}", S::LABEL, len)
    }
}

impl<S, C> EpochMap<S::IndexKey, MetadataStoreObject<S, C>>
where
    S: Spec + PartialEq,
    S::Status: PartialEq,
    C: MetadataItem + PartialEq,
{
    fn insert_meta(
        &mut self,
        value: MetadataStoreObject<S, C>,
    ) -> Option<EpochCounter<MetadataStoreObject<S, C>>> {
        self.insert(value.key_owned(), value)
    }
}

pub struct SyncStatus {
    pub epoch: Epoch,
    pub add: i32,
    pub update: i32,
    pub delete: i32,
}

impl<S, C> LocalStore<S, C>
where
    S: Spec + PartialEq,
    S::Status: PartialEq,
    S::IndexKey: Display,
    C: MetadataItem + PartialEq,
{
    /// sync list of changes with with this store assuming incoming is source of truth.
    /// any objects not in incoming list will be deleted (but will be in history).
    /// after sync operation, prior history will be removed and any subsequent
    /// change query will return full list instead of changes
    pub async fn sync_all(&self, incoming_changes: Vec<MetadataStoreObject<S, C>>) -> SyncStatus {
        let read_guard = self.read().await;

        debug!(
            "SyncAll: <{}> epoch: {} incoming {}",
            S::LABEL,
            read_guard.epoch(),
            incoming_changes.len()
        );

        let mut local_keys = read_guard.clone_keys();

        drop(read_guard);

        let (mut add_cnt, mut mod_cnt, mut del_cnt) = (0, 0, 0);

        let mut write_guard = self.write().await;
        write_guard.increment_epoch();

        for source in incoming_changes {
            let key = source.key().clone();
            // always insert, so we stamp current epoch
            if let Some(old_value) = write_guard.insert_meta(source) {
                if old_value.inner() != write_guard.get(&key).unwrap().inner() {
                    mod_cnt += 1;
                }
            } else {
                add_cnt += 1;
            }

            local_keys.retain(|n| n != &key);
        }

        // delete value that shouldn't be there
        for name in local_keys.into_iter() {
            if write_guard.contains_key(&name) {
                if let Some(_) = write_guard.remove(&name) {
                    del_cnt += 1;
                } else {
                    error!("delete  should never fail since key exists: {:#?}", name);
                }
            } else {
                error!("kv unexpectedly removed... skipped {:#?}", name);
            }
        }

        write_guard.mark_fence();

        let epoch = write_guard.epoch();
        drop(write_guard);

        debug!(
            "Sync all: <{}:{}> [add:{}, mod:{}, del:{}], ",
            S::LABEL,
            epoch,
            add_cnt,
            mod_cnt,
            del_cnt,
        );
        SyncStatus {
            epoch,
            add: add_cnt,
            update: mod_cnt,
            delete: del_cnt,
        }
    }

    /// apply changes to this store
    /// this assume changes are source of truth.
    /// if item doesn't exit, it will be treated as add
    /// if item exist but different, it will be treated as updated
    /// epoch will be only incremented if there are actual changes
    /// which means this is idempotent operations.
    /// same add result in only 1 single epoch increase.
    pub async fn apply_changes(&self, changes: Vec<LSUpdate<S, C>>) -> Option<SyncStatus> {
        let read_guard = self.read().await;
        debug!(
            "ApplyChanges <{}> epoch: {}, incoming: {} items",
            S::LABEL,
            read_guard.epoch(),
            changes.len(),
        );

        let mut actual_changes = vec![];

        // perform dry run to see if there are real changes
        for dry_run_change in changes.into_iter() {
            match dry_run_change {
                LSUpdate::Mod(new_kv_value) => {
                    if let Some(old_value) = read_guard.get(new_kv_value.key()) {
                        if old_value.inner() != &new_kv_value {
                            // existing but different, transform into mod
                            actual_changes.push(LSUpdate::Mod(new_kv_value));
                        }
                    } else {
                        actual_changes.push(LSUpdate::Mod(new_kv_value));
                    }
                }
                LSUpdate::Delete(key) => {
                    if read_guard.contains_key(&key) {
                        actual_changes.push(LSUpdate::Delete(key));
                    }
                }
            }
        }

        drop(read_guard);

        if actual_changes.len() == 0 {
            debug!("Apply changes <{}> required no changes", S::LABEL);
            return None;
        }

        let (mut add_cnt, mut mod_cnt, mut del_cnt) = (0, 0, 0);
        let mut write_guard = self.write().await;
        write_guard.increment_epoch();

        // loop through items and generate add/mod actions
        for change in actual_changes.into_iter() {
            match change {
                LSUpdate::Mod(new_kv_value) => {
                    if let Some(_) = write_guard.insert_meta(new_kv_value) {
                        mod_cnt += 1;
                    } else {
                        // there was no existing, so this is new
                        add_cnt += 1;
                    }
                }
                LSUpdate::Delete(key) => {
                    write_guard.remove(&key);
                    del_cnt += 1;
                }
            }
        }

        let epoch = write_guard.epoch();
        drop(write_guard);

        debug!(
            "Apply changes {} [add:{},mod:{},del:{},epoch: {}",
            S::LABEL,
            add_cnt,
            mod_cnt,
            del_cnt,
            epoch,
        );
        Some(SyncStatus {
            epoch,
            add: add_cnt,
            update: mod_cnt,
            delete: del_cnt,
        })
    }
}

#[cfg(test)]
pub mod test {

    use flv_future_aio::test_async;

    use crate::topic::*;
    use crate::topic::store::*;
    use crate::topic::store::DefaultTopicMd;

    use super::*;

    #[test_async]
    async fn test_store_check_items_against_empty() -> Result<(), ()> {
        let topics = vec![DefaultTopicMd::with_spec("topic1", TopicSpec::default())];
        let topic_store = DefaultTopicLocalStore::default();
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
        let topic_store = DefaultTopicLocalStore::bulk_new(vec![DefaultTopicMd::with_spec(
            "topic1",
            TopicSpec::default(),
        )]);

        let status = topic_store
            .sync_all(vec![DefaultTopicMd::with_spec(
                "topic1",
                TopicSpec::default(),
            )])
            .await;
        assert_eq!(status.epoch, 1);

        Ok(())
    }

    #[test_async]
    async fn test_store_sync_all() -> Result<(), ()> {
        let topic1 = DefaultTopicMd::with_spec("topic1", TopicSpec::default());
        let topic2 = DefaultTopicMd::with_spec("topic2", TopicSpec::default());

        let topic_store = DefaultTopicLocalStore::bulk_new(vec![topic1.clone(), topic2.clone()]);
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
        let old_topic = DefaultTopicMd::with_spec("topic1", TopicSpec::default());

        let topic_store = DefaultTopicLocalStore::bulk_new(vec![old_topic]);
        assert_eq!(topic_store.epoch().await, 0);

        // same topic key but with different status
        let mut status = TopicStatus::default();
        status.resolution = TopicResolution::Provisioned;
        let updated_topic = DefaultTopicMd::new("topic1", TopicSpec::default(), status);

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
        let topic_store = DefaultTopicLocalStore::bulk_new(vec![DefaultTopicMd::with_spec(
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
        let new_topic = DefaultTopicMd::with_spec("topic1", TopicSpec::default());

        let changes = vec![LSUpdate::Mod(new_topic.clone())];

        let topic_store = DefaultTopicLocalStore::default();

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
        let topic_store = DefaultTopicLocalStore::default();
        assert_eq!(topic_store.epoch().await, 0);

        let topic1 = DefaultTopicMd::with_spec("topic1", TopicSpec::default());
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
        let topic2 = DefaultTopicMd::with_spec("topic2", TopicSpec::default());
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
                DefaultTopicMd::with_spec("topic2", TopicSpec::default())
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
