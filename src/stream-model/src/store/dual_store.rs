use std::sync::Arc;
use std::fmt::Debug;
use std::fmt::Display;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use tracing::debug;
use tracing::error;
use async_rwlock::RwLock;
use async_rwlock::RwLockReadGuard;
use async_rwlock::RwLockWriteGuard;

use crate::core::{MetadataItem,Spec};
use super::MetadataStoreObject;
use super::{ DualEpochMap, DualEpochCounter, Epoch };
use super::actions::LSUpdate;

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
pub struct DualLocalStore<S, C>(RwLock<DualEpochMap<S::IndexKey, MetadataStoreObject<S, C>>>)
where
    S: Spec,
    C: MetadataItem;

impl<S, C> Default for DualLocalStore<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    fn default() -> Self {
        Self(RwLock::new(DualEpochMap::new()))
    }
}

impl<S, C> DualLocalStore<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    /// initialize local stores with list of metadata objects
    pub fn bulk_new<N>(objects: Vec<N>) -> Self
    where
        N: Into<MetadataStoreObject<S, C>>,
    {
        let obj: Vec<MetadataStoreObject<S, C>> = objects.into_iter().map(|s| s.into()).collect();
        let mut map = HashMap::new();
        for obj in obj {
            map.insert(obj.key.clone(), obj.into());
        }
        Self(RwLock::new(DualEpochMap::new_with_map(map)))
    }

    /// create arc wrapper
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Read guard
    #[inline(always)]
    pub async fn read<'a>(
        &'_ self,
    ) -> RwLockReadGuard<'_, DualEpochMap<S::IndexKey, MetadataStoreObject<S, C>>> {
        self.0.read().await
    }

    /// write guard, this is private, use sync API to make changes
    #[inline(always)]
    pub async fn write<'a>(
        &'_ self,
    ) -> RwLockWriteGuard<'_, DualEpochMap<S::IndexKey, MetadataStoreObject<S, C>>> {
        self.0.write().await
    }

    /// current epoch
    pub async fn epoch(&self) -> i64 {
        self.read().await.epoch()
    }

    /// initial epoch that should be used
    /// store will always have epoch greater than init_epoch if there are any changes
    pub fn init_epoch(&self) -> DualEpochCounter<()> {
        DualEpochCounter::default()
    }

    /// copy of the value
    pub async fn value<K: ?Sized>(&self, key: &K) -> Option<DualEpochCounter<MetadataStoreObject<S, C>>>
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

impl<S, C> Display for DualLocalStore<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} Store", S::LABEL)
    }
}

impl<S, C> DualEpochMap<S::IndexKey, MetadataStoreObject<S, C>>
where
    S: Spec + PartialEq,
    S::Status: PartialEq,
    C: MetadataItem + PartialEq,
{
    fn insert_meta(
        &mut self,
        value: MetadataStoreObject<S, C>,
    ) -> Option<DualEpochCounter<MetadataStoreObject<S, C>>> {
        self.insert(value.key_owned(), value)
    }
}

pub struct SyncStatus {
    pub epoch: Epoch,
    pub add: i32,
    pub update_spec: i32,
    pub update_status: i32,
    pub delete: i32,
}

impl<S, C> DualLocalStore<S, C>
where
    S: Spec + PartialEq,
    S::Status: PartialEq,
    S::IndexKey: Display,
    C: MetadataItem + PartialEq,
{
    /// sync with incoming changes as source of truth.
    /// any objects not in incoming list will be deleted.
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

        let (mut add, mut update_spec, mut update_status, mut delete) = (0, 0, 0, 0);

        let mut write_guard = self.write().await;
        // start new epoch cycle
        write_guard.increment_epoch();

        for source in incoming_changes {
            let key = source.key().clone();
            // always insert, so we stamp current epoch
            if let Some(old_value) = write_guard.insert(key,) {
                let changes = old_value
                    .inner()
                    .diff(write_guard.get(&key).unwrap().inner());
                if changes.spec {
                    update_spec += 1;
                }
                if changes.status {
                    update_status += 1;
                }
            } else {
                add += 1;
            }

            local_keys.retain(|n| n != &key);
        }

        // delete value that shouldn't be there
        for name in local_keys.into_iter() {
            if write_guard.contains_key(&name) {
                if write_guard.remove(&name).is_some() {
                    delete += 1;
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
            "Sync all: <{}:{}> [add:{}, mod_spec:{}, mod_status: {}, del:{}], ",
            S::LABEL,
            epoch,
            add,
            update_spec,
            update_status,
            delete,
        );
        SyncStatus {
            epoch,
            add,
            update_spec,
            update_status,
            delete,
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
                        if new_kv_value.is_newer(old_value) {
                            // only replace if new kv is newer than old
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

        if actual_changes.is_empty() {
            debug!("Apply changes <{}> required no changes", S::LABEL);
            return None;
        }

        let (mut add, mut update_spec, mut update_status, mut delete) = (0, 0, 0, 0);
        let mut write_guard = self.write().await;
        write_guard.increment_epoch();

        // loop through items and generate add/mod actions
        for change in actual_changes.into_iter() {
            match change {
                LSUpdate::Mod(new_kv_value) => {
                    let key = new_kv_value.key_owned();
                    if let Some(old_value) = write_guard.insert_meta(new_kv_value) {
                        let changes = old_value
                            .inner()
                            .diff(write_guard.get(&key).unwrap().inner());
                        if changes.spec {
                            update_spec += 1;
                        }
                        if changes.status {
                            update_status += 1;
                        }
                    } else {
                        // there was no existing, so this is new
                        add += 1;
                    }
                }
                LSUpdate::Delete(key) => {
                    write_guard.remove(&key);
                    delete += 1;
                }
            }
        }

        let epoch = write_guard.epoch();
        drop(write_guard);

        debug!(
            "Apply changes {} [add:{},mod_spec:{},mod_status: {},del:{},epoch: {}",
            S::LABEL,
            add,
            update_spec,
            update_status,
            delete,
            epoch,
        );
        Some(SyncStatus {
            epoch,
            add,
            update_spec,
            update_status,
            delete,
        })
    }
}
