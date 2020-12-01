use std::sync::Arc;
use std::fmt::Debug;
use std::fmt::Display;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use tracing::{debug, trace, error};
use async_rwlock::RwLock;
use async_rwlock::RwLockReadGuard;
use async_rwlock::RwLockWriteGuard;

use crate::core::{MetadataItem, Spec};
use super::MetadataStoreObject;
use super::{DualEpochMap, DualEpochCounter, Epoch, EpochChanges};
use super::actions::LSUpdate;
use super::event::{EventPublisher, ChangeListener};

/// Idempotent local memory cache of meta objects.
/// There are only 2 write operations are permitted: sync and apply changes which are idempotent.
/// For read, read guards are provided which provide hash map API using deref.  
/// Hash values are wrapped in EpochCounter.  EpochCounter is also deref.
/// Using async lock to ensure read/write are thread safe.
#[derive(Debug)]
pub struct LocalStore<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    store: RwLock<DualEpochMap<S::IndexKey, MetadataStoreObject<S, C>>>,
    spec_publisher: Arc<EventPublisher>,
    status_publisher: Arc<EventPublisher>,
}

impl<S, C> Default for LocalStore<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    fn default() -> Self {
        Self {
            store: RwLock::new(DualEpochMap::new()),
            spec_publisher: EventPublisher::shared(),
            status_publisher: EventPublisher::shared(),
        }
    }
}

impl<S, C> LocalStore<S, C>
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
        Self {
            store: RwLock::new(DualEpochMap::new_with_map(map)),
            spec_publisher: EventPublisher::shared(),
            status_publisher: EventPublisher::shared(),
        }
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
        self.store.read().await
    }

    /// write guard, this is private, use sync API to make changes
    #[inline(always)]
    pub async fn write<'a>(
        &'_ self,
    ) -> RwLockWriteGuard<'_, DualEpochMap<S::IndexKey, MetadataStoreObject<S, C>>> {
        self.store.write().await
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
    pub async fn value<K: ?Sized>(
        &self,
        key: &K,
    ) -> Option<DualEpochCounter<MetadataStoreObject<S, C>>>
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

    pub fn spec_change_listener(&self) -> ChangeListener {
        self.spec_publisher.change_listener()
    }

    pub fn status_change_listener(&self) -> ChangeListener {
        self.status_publisher.change_listener()
    }

    /// find spec changes given change listener
    /// reset change listener to latest epoch
    pub async fn spec_changes_since(
        &self,
        change_listener: &mut ChangeListener,
    ) -> EpochChanges<MetadataStoreObject<S, C>> {
        let last_change = change_listener.last_change();
        let read_guard = self.read().await;
        let changes = read_guard.spec_changes_since(last_change);
        drop(read_guard);
        trace!(
            "finding last spec change: {}, from: {}",
            last_change,
            changes.epoch
        );
        change_listener.set_last_change(changes.epoch);
        changes
    }

    pub async fn status_changes_since(
        &self,
        change_listener: &mut ChangeListener,
    ) -> EpochChanges<MetadataStoreObject<S, C>> {
        let last_change = change_listener.last_change();
        let read_guard = self.read().await;
        let changes = read_guard.status_changes_since(last_change);
        drop(read_guard);
        trace!(
            "finding last status change: {}, from: {}",
            last_change,
            changes.epoch
        );
        change_listener.set_last_change(changes.epoch);
        changes
    }

    /// find changes if either  spec and status changes and resync both them
    pub async fn all_changes_since(
        &self,
        spec_change: &mut ChangeListener,
        status_change: &mut ChangeListener,
    ) -> EpochChanges<MetadataStoreObject<S, C>> {
        let last_change = std::cmp::min(spec_change.last_change(), status_change.last_change());
        let read_guard = self.read().await;
        let changes = read_guard.changes_since(last_change);
        drop(read_guard);
        trace!(
            "finding last change: {}, from: {}",
            last_change,
            changes.epoch
        );
        spec_change.set_last_change(changes.epoch);
        status_change.set_last_change(changes.epoch);
        changes
    }
}

impl<S, C> Display for LocalStore<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} Store", S::LABEL)
    }
}

pub struct SyncStatus {
    pub epoch: Epoch,
    pub add: i32,
    pub update_spec: i32,
    pub update_status: i32,
    pub delete: i32,
}

impl SyncStatus {
    pub fn has_spec_changes(&self) -> bool {
        self.add > 0 || self.update_spec > 0 || self.delete > 0
    }

    pub fn has_status_changes(&self) -> bool {
        self.update_status > 0
    }
}

impl<S, C> LocalStore<S, C>
where
    S: Spec + PartialEq,
    S::Status: PartialEq,
    S::IndexKey: Display,
    C: MetadataItem + PartialEq,
{
    /// sync with incoming changes as source of truth.
    /// any objects not in incoming list will be deleted
    /// after sync operation, prior history will be removed and any subsequent
    /// change query will return full list instead of changes
    pub async fn sync_all(&self, incoming_changes: Vec<MetadataStoreObject<S, C>>) -> SyncStatus {
        let (mut add, mut update_spec, mut update_status, mut delete) = (0, 0, 0, 0);

        let mut write_guard = self.write().await;

        debug!(
            "SyncAll: <{}> epoch: {} incoming {}",
            S::LABEL,
            write_guard.epoch(),
            incoming_changes.len()
        );

        let mut local_keys = write_guard.clone_keys();
        // start new epoch cycle
        write_guard.increment_epoch();

        for source in incoming_changes {
            let key = source.key().clone();

            // always insert, so we stamp current epoch
            if let Some(diff) = write_guard.update(key.clone(), source) {
                if diff.spec {
                    update_spec += 1;
                }
                if diff.status {
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

        let status = SyncStatus {
            epoch,
            add,
            update_spec,
            update_status,
            delete,
        };

        drop(write_guard);

        if status.has_spec_changes() {
            self.spec_publisher.store_change(epoch);
            self.spec_publisher.notify();
        }

        if status.has_status_changes() {
            self.status_publisher.store_change(epoch);
            self.status_publisher.notify();
        }

        debug!(
            "Sync all: <{}:{}> [add:{}, mod_spec:{}, mod_status: {}, del:{}], ",
            S::LABEL,
            epoch,
            add,
            update_spec,
            update_status,
            delete,
        );
        status
    }

    /// apply changes to this store
    /// if item doesn't exit, it will be treated as add
    /// if item exist but different, it will be treated as updated
    /// epoch will be only incremented if there are actual changes
    /// which means this is idempotent operations.
    /// same add result in only 1 single epoch increase.
    pub async fn apply_changes(&self, changes: Vec<LSUpdate<S, C>>) -> Option<SyncStatus> {
        let (mut add, mut update_spec, mut update_status, mut delete) = (0, 0, 0, 0);
        let mut write_guard = self.write().await;
        write_guard.increment_epoch();

        debug!(
            "apply changes <{}> new epoch: {}, incoming: {} items",
            S::LABEL,
            write_guard.epoch(),
            changes.len(),
        );

        // loop through items and generate add/mod actions
        for change in changes.into_iter() {
            match change {
                LSUpdate::Mod(new_kv_value) => {
                    let key = new_kv_value.key_owned();
                    if let Some(diff) = write_guard.update(key, new_kv_value) {
                        if diff.spec {
                            update_spec += 1;
                        }
                        if diff.status {
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

        // if there are no changes, we revert epoch
        if add == 0 && update_spec == 0 && update_status == 0 && delete == 0 {
            write_guard.decrement_epoch();

            debug!(
                "Apply changes: {} no changes, reverting back epoch to: {}",
                S::LABEL,
                write_guard.epoch()
            );

            return None;
        }

        let epoch = write_guard.epoch();

        let status = SyncStatus {
            epoch,
            add,
            update_spec,
            update_status,
            delete,
        };

        drop(write_guard);

        if status.has_spec_changes() {
            trace!("notify spec changed: {}", epoch);
            self.spec_publisher.store_change(epoch);
            self.spec_publisher.notify();
        }

        if status.has_status_changes() {
            trace!("notify status changed: {}", epoch);
            self.status_publisher.store_change(epoch);
            self.status_publisher.notify();
        }

        debug!(
            "Apply changes {} [add:{},mod_spec:{},mod_status: {},del:{},epoch: {}",
            S::LABEL,
            add,
            update_spec,
            update_status,
            delete,
            epoch,
        );
        Some(status)
    }
}

#[cfg(test)]
mod test {

    use fluvio_future::test_async;

    use crate::store::actions::LSUpdate;
    use crate::test_fixture::{TestSpec, TestStatus, DefaultTest};

    use super::LocalStore;

    type DefaultTestStore = LocalStore<TestSpec, u32>;

    #[test_async]
    async fn test_store_sync_all() -> Result<(), ()> {
        let tests = vec![DefaultTest::with_spec("t1", TestSpec::default())];
        let test_store = DefaultTestStore::default();
        assert_eq!(test_store.epoch().await, 0);

        let sync1 = test_store.sync_all(tests.clone()).await;
        assert_eq!(test_store.epoch().await, 1);
        assert_eq!(sync1.add, 1);
        assert_eq!(sync1.delete, 0);
        assert_eq!(sync1.update_spec, 0);
        assert_eq!(sync1.update_status, 0);

        let read_guard = test_store.read().await;
        let test1 = read_guard.get("t1").expect("t1 should exists");
        assert_eq!(test1.status_epoch(), 1);
        assert_eq!(test1.spec_epoch(), 1);
        drop(read_guard);

        // apply same changes should have no effect

        let sync2 = test_store
            .sync_all(vec![DefaultTest::with_spec("t1", TestSpec { replica: 6 })])
            .await;
        assert_eq!(test_store.epoch().await, 2);
        assert_eq!(sync2.add, 0);
        assert_eq!(sync2.delete, 0);
        assert_eq!(sync2.update_spec, 1);
        assert_eq!(sync2.update_status, 0);

        Ok(())
    }

    #[test_async]
    async fn test_store_update() -> Result<(), ()> {
        let initial_topic = DefaultTest::with_spec("t1", TestSpec::default()).with_context(2);

        let topic_store = DefaultTestStore::default();
        let _ = topic_store.sync_all(vec![initial_topic.clone()]).await;
        assert_eq!(topic_store.epoch().await, 1);

        // applying same data should result in change since version stays same
        assert!(topic_store
            .apply_changes(vec![LSUpdate::Mod(initial_topic.clone())])
            .await
            .is_none());

        // applying updated version with but same data result in no changes
        let topic2 = DefaultTest::with_spec("t1", TestSpec::default()).with_context(3);
        assert!(topic_store
            .apply_changes(vec![LSUpdate::Mod(topic2)])
            .await
            .is_none());
        assert_eq!(topic_store.epoch().await, 1); // still same epoch
        let read_guard = topic_store.read().await;
        let t = read_guard.get("t1").expect("t1");
        assert_eq!(t.spec_epoch(), 1);
        drop(read_guard);

        let topic3 =
            DefaultTest::new("t1", TestSpec::default(), TestStatus { up: true }).with_context(3);
        let changes = topic_store
            .apply_changes(vec![LSUpdate::Mod(topic3)])
            .await
            .expect("some changes");
        assert_eq!(changes.update_spec, 0);
        assert_eq!(changes.update_status, 1);
        Ok(())
    }
}

#[cfg(test)]
mod test_notify {

    use std::sync::Arc;
    use std::time::Duration;
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering::SeqCst;

    use tracing::debug;

    use fluvio_future::test_async;
    use fluvio_future::task::spawn;
    use fluvio_future::timer::sleep;

    use crate::store::actions::LSUpdate;
    use crate::store::event::SimpleEvent;
    use crate::test_fixture::{TestSpec, DefaultTest};

    use super::LocalStore;

    type DefaultTestStore = LocalStore<TestSpec, u32>;

    use super::ChangeListener;

    struct TestController {
        store: Arc<DefaultTestStore>,
        shutdown: Arc<SimpleEvent>,
        last_change: Arc<AtomicI64>,
    }

    impl TestController {
        fn start(
            ctx: Arc<DefaultTestStore>,
            shutdown: Arc<SimpleEvent>,
            last_change: Arc<AtomicI64>,
        ) {
            let controller = Self {
                store: ctx,
                shutdown,
                last_change,
            };

            spawn(controller.dispatch_loop());
        }

        async fn dispatch_loop(mut self) {
            use tokio::select;

            debug!("entering loop");

            let mut spec_listner = self.store.spec_change_listener();

            loop {
                self.sync(&mut spec_listner).await;

                select! {
                    _ = spec_listner.listen() => {
                        debug!("spec change occur: {}",spec_listner.last_change());
                        continue;
                    },
                    _ = self.shutdown.listen() => {
                        debug!("shutdown");
                        break;
                    }
                }
            }
        }

        async fn sync(&mut self, spec_listner: &mut ChangeListener) {
            debug!("sync start");
            let (update, _delete) = self.store.spec_changes_since(spec_listner).await.parts();
            // assert!(update.len() > 0);
            debug!("changes: {}", update.len());
            sleep(Duration::from_millis(10)).await;
            debug!("sync end");
            self.last_change.fetch_add(1, SeqCst);
        }
    }

    #[test_async]
    async fn test_store_notifications() -> Result<(), ()> {
        let topic_store = Arc::new(DefaultTestStore::default());
        let last_change = Arc::new(AtomicI64::new(0));
        let shutdown = SimpleEvent::shared();

        TestController::start(topic_store.clone(), shutdown.clone(), last_change.clone());

        let initial_topic = DefaultTest::with_spec("t1", TestSpec::default()).with_context(2);
        let _ = topic_store.sync_all(vec![initial_topic.clone()]).await;

        for i in 0..10u16 {
            sleep(Duration::from_millis(2)).await;
            let topic_name = format!("topic{}", i);
            debug!("creating topic: {}", topic_name);
            let topic = DefaultTest::with_spec(topic_name, TestSpec::default()).with_context(3);
            let _ = topic_store.apply_changes(vec![LSUpdate::Mod(topic)]).await;
        }

        // wait for controller to sync
        sleep(Duration::from_millis(100)).await;
        shutdown.notify();
        sleep(Duration::from_millis(1)).await;

        assert_eq!(last_change.load(SeqCst), 4);

        Ok(())
    }
}
