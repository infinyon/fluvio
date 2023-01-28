use std::sync::Arc;
use std::fmt::Debug;
use std::fmt::Display;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use tracing::trace;
use tracing::{debug, error};
use async_rwlock::RwLock;
use async_rwlock::RwLockReadGuard;
use async_rwlock::RwLockWriteGuard;

use crate::core::{MetadataItem, Spec};

use super::MetadataStoreObject;
use super::{DualEpochMap, DualEpochCounter, Epoch, EpochChanges};
use super::actions::LSUpdate;
use super::event::EventPublisher;

pub use listener::ChangeListener;
pub type MetadataChanges<S, C> = EpochChanges<MetadataStoreObject<S, C>>;

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
    event_publisher: Arc<EventPublisher>,
}

impl<S, C> Default for LocalStore<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    fn default() -> Self {
        Self {
            store: RwLock::new(DualEpochMap::new()),
            event_publisher: EventPublisher::shared(),
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
            event_publisher: EventPublisher::shared(),
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
    async fn write<'a>(
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
        self.read().await.get(key).cloned()
    }

    /// copy spec
    pub async fn spec<K: ?Sized>(&self, key: &K) -> Option<S>
    where
        S::IndexKey: Borrow<K>,
        K: Eq + Hash,
    {
        self.read().await.get(key).map(|value| value.spec.clone())
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

    pub async fn count(&self) -> usize {
        self.read().await.len()
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

    pub fn event_publisher(&self) -> &EventPublisher {
        &self.event_publisher
    }

    /// create new change listener
    pub fn change_listener(self: &Arc<Self>) -> ChangeListener<S, C> {
        ChangeListener::new(self.clone())
    }

    /// returns once there is at least one change recorded by the the event_publisher
    pub async fn wait_for_first_change(self: &Arc<Self>) {
        self.change_listener().listen().await;
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
    pub update_meta: i32,
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
        let (mut add, mut update_spec, mut update_status, mut update_meta, mut delete) =
            (0, 0, 0, 0, 0);

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
                if diff.meta {
                    update_meta += 1;
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
            update_meta,
            delete,
        };

        drop(write_guard);

        self.event_publisher.store_change(epoch);

        debug!(
            "Sync all: <{}:{}> [add:{}, mod_spec:{}, mod_status: {}, mod_meta: {}, del:{}], ",
            S::LABEL,
            epoch,
            add,
            update_spec,
            update_status,
            update_meta,
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
        let (mut add, mut update_spec, mut update_status, mut update_meta, mut delete) =
            (0, 0, 0, 0, 0);
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
                        if diff.meta {
                            update_meta += 1;
                        }
                        trace!(update_spec, update_status, update_meta);
                    } else {
                        trace!("new");
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
        if add == 0 && update_spec == 0 && update_status == 0 && delete == 0 && update_meta == 0 {
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
            update_meta,
            delete,
        };

        drop(write_guard);

        debug!("notify epoch changed: {}", epoch);
        self.event_publisher.store_change(epoch);

        debug!(
            "Apply changes {} [add:{},mod_spec:{},mod_status: {},mod_update: {}, del:{},epoch: {}",
            S::LABEL,
            add,
            update_spec,
            update_status,
            update_meta,
            delete,
            epoch,
        );
        Some(status)
    }
}

mod listener {

    use std::fmt;
    use std::sync::Arc;

    use tracing::{trace, debug, instrument};

    use crate::store::event::EventPublisher;
    use crate::store::{
        ChangeFlag, FULL_FILTER, META_FILTER, MetadataStoreObject, SPEC_FILTER, STATUS_FILTER,
    };

    use super::{LocalStore, Spec, MetadataItem, MetadataChanges};

    /// listen for changes local store
    pub struct ChangeListener<S, C>
    where
        S: Spec,
        C: MetadataItem,
    {
        store: Arc<LocalStore<S, C>>,
        last_change: i64,
    }

    impl<S, C> fmt::Debug for ChangeListener<S, C>
    where
        S: Spec,
        C: MetadataItem,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "{} last:{},current:{}",
                S::LABEL,
                self.last_change,
                self.event_publisher().current_change()
            )
        }
    }

    impl<S, C> ChangeListener<S, C>
    where
        S: Spec,
        C: MetadataItem,
    {
        pub fn new(store: Arc<LocalStore<S, C>>) -> Self {
            Self {
                store,
                last_change: 0,
            }
        }

        #[inline]
        fn event_publisher(&self) -> &EventPublisher {
            self.store.event_publisher()
        }

        /// check if there should be any changes
        /// this should be done before event listener
        /// to ensure no events are missed
        #[inline]
        pub fn has_change(&self) -> bool {
            self.event_publisher().current_change() > self.last_change
        }

        /// sync change to current change
        #[inline(always)]
        pub fn load_last(&mut self) {
            self.set_last_change(self.event_publisher().current_change());
        }

        #[inline(always)]
        pub fn set_last_change(&mut self, updated_change: i64) {
            self.last_change = updated_change;
        }

        #[inline]
        pub fn last_change(&self) -> i64 {
            self.last_change
        }

        pub fn current_change(&self) -> i64 {
            self.event_publisher().current_change()
        }

        pub async fn listen(&self) {
            if self.has_change() {
                trace!("before has change: {}", self.last_change());
                return;
            }

            let listener = self.event_publisher().listen();

            if self.has_change() {
                trace!("after has change: {}", self.last_change());
                return;
            }

            trace!("waiting for publisher");

            listener.await;

            trace!("new change: {}", self.current_change());
        }

        /// find all changes derived from this listener
        pub async fn sync_changes(&mut self) -> MetadataChanges<S, C> {
            self.sync_changes_with_filter(&FULL_FILTER).await
        }

        /// find all spec related changes
        pub async fn sync_spec_changes(&mut self) -> MetadataChanges<S, C> {
            self.sync_changes_with_filter(&SPEC_FILTER).await
        }

        /// all status related changes
        pub async fn sync_status_changes(&mut self) -> MetadataChanges<S, C> {
            self.sync_changes_with_filter(&STATUS_FILTER).await
        }

        /// all meta related changes
        pub async fn sync_meta_changes(&mut self) -> MetadataChanges<S, C> {
            self.sync_changes_with_filter(&META_FILTER).await
        }

        /// all meta related changes
        pub async fn sync_changes_with_filter(
            &mut self,
            filter: &ChangeFlag,
        ) -> MetadataChanges<S, C> {
            let read_guard = self.store.read().await;
            let changes = read_guard.changes_since_with_filter(self.last_change, filter);
            drop(read_guard);
            trace!(
                "finding last status change: {}, from: {}",
                self.last_change,
                changes.epoch
            );

            let current_epoch = self.event_publisher().current_change();
            if changes.epoch > current_epoch {
                trace!(
                    "latest epoch: {} > spec epoch: {}",
                    changes.epoch,
                    current_epoch
                );
            }
            self.set_last_change(changes.epoch);
            changes
        }

        /// wait for initial loading and return all as expected
        #[instrument()]
        pub async fn wait_for_initial_sync(&mut self) -> Vec<MetadataStoreObject<S, C>> {
            debug!("waiting");
            self.listen().await;

            let changes = self.sync_changes().await;
            assert!(changes.is_sync_all());

            debug!("finished initial sync");
            changes.parts().0
        }
    }
}

#[cfg(test)]
mod test {

    use crate::store::actions::LSUpdate;
    use crate::fixture::{TestSpec, TestStatus, DefaultTest, TestMeta};

    use super::LocalStore;

    type DefaultTestStore = LocalStore<TestSpec, TestMeta>;

    #[fluvio_future::test]
    async fn test_store_sync_all() {
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

        // sync all with spec changes only

        let spec_changes =
            vec![DefaultTest::with_spec("t1", TestSpec { replica: 6 }).with_context(2)];
        let sync2 = test_store.sync_all(spec_changes.clone()).await;
        assert_eq!(test_store.epoch().await, 2);
        assert_eq!(sync2.add, 0);
        assert_eq!(sync2.delete, 0);
        assert_eq!(sync2.update_spec, 1);
        assert_eq!(sync2.update_status, 0);

        // apply again, this time there should not be any change all
        let sync3 = test_store.sync_all(spec_changes.clone()).await;
        assert_eq!(test_store.epoch().await, 3);
        assert_eq!(sync3.add, 0);
        assert_eq!(sync3.delete, 0);
        assert_eq!(sync3.update_spec, 0);
        assert_eq!(sync3.update_status, 0);
    }

    #[fluvio_future::test]
    async fn test_store_update() {
        let initial_topic = DefaultTest::with_spec("t1", TestSpec::default()).with_context(2);

        let topic_store = DefaultTestStore::default();
        let _ = topic_store.sync_all(vec![initial_topic.clone()]).await;
        assert_eq!(topic_store.epoch().await, 1);

        // applying same data should result in zero changes in the store
        assert!(topic_store
            .apply_changes(vec![LSUpdate::Mod(initial_topic.clone())])
            .await
            .is_none());
        assert_eq!(topic_store.epoch().await, 1);

        // update spec should result in increase epoch
        let topic2 =
            DefaultTest::new("t1", TestSpec::default(), TestStatus { up: true }).with_context(3);
        let changes = topic_store
            .apply_changes(vec![LSUpdate::Mod(topic2)])
            .await
            .expect("some changes");
        assert_eq!(changes.update_spec, 0);
        assert_eq!(changes.update_status, 1);
        assert_eq!(topic_store.epoch().await, 2);
        assert_eq!(
            topic_store.value("t1").await.expect("t1").ctx().item().rev,
            3
        );

        // updating topics should only result in epoch

        assert_eq!(initial_topic.ctx().item().rev, 2);
        let changes = topic_store
            .apply_changes(vec![LSUpdate::Mod(initial_topic.clone())])
            .await;
        assert_eq!(topic_store.epoch().await, 2);
        assert!(changes.is_none());
        assert_eq!(
            topic_store.value("t1").await.expect("t1").status,
            TestStatus { up: true }
        );

        // re-syching with initial topic should only cause epoch to change
        let sync_all = topic_store.sync_all(vec![initial_topic]).await;
        assert_eq!(topic_store.epoch().await, 3);
        assert_eq!(sync_all.add, 0);
        assert_eq!(sync_all.delete, 0);
        assert_eq!(sync_all.update_spec, 0);
        assert_eq!(sync_all.update_status, 0);
    }
}

#[cfg(test)]
mod test_notify {

    use std::sync::Arc;
    use std::time::Duration;
    use std::sync::atomic::{AtomicI64, AtomicBool};
    use std::sync::atomic::Ordering::SeqCst;

    use async_std::task::JoinHandle;
    use tokio::select;
    use tracing::debug;

    use fluvio_future::task::spawn;
    use fluvio_future::timer::sleep;

    use crate::core::{Spec, MetadataItem};
    use crate::store::actions::LSUpdate;
    use crate::store::event::SimpleEvent;
    use crate::fixture::{TestSpec, DefaultTest, TestMeta};

    use super::LocalStore;

    type DefaultTestStore = LocalStore<TestSpec, TestMeta>;

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
            debug!("entering loop");

            let mut spec_listener = self.store.change_listener();

            loop {
                self.sync(&mut spec_listener).await;

                select! {
                    _ = spec_listener.listen() => {
                        debug!("spec change occur: {}",spec_listener.last_change());
                        continue;
                    },
                    _ = self.shutdown.listen() => {
                        debug!("shutdown");
                        break;
                    }
                }
            }
        }

        async fn sync(&mut self, spec_listener: &mut ChangeListener<TestSpec, TestMeta>) {
            debug!("sync start");
            let (update, _delete) = spec_listener.sync_spec_changes().await.parts();
            // assert!(update.len() > 0);
            debug!("changes: {}", update.len());
            sleep(Duration::from_millis(10)).await;
            debug!("sync end");
            self.last_change.fetch_add(1, SeqCst);
        }
    }

    #[fluvio_future::test]
    async fn test_store_notifications() {
        let topic_store = Arc::new(DefaultTestStore::default());
        let last_change = Arc::new(AtomicI64::new(0));
        let shutdown = SimpleEvent::shared();

        TestController::start(topic_store.clone(), shutdown.clone(), last_change.clone());

        let initial_topic = DefaultTest::with_spec("t1", TestSpec::default()).with_context(2);
        let _ = topic_store.sync_all(vec![initial_topic.clone()]).await;

        for i in 0..10u16 {
            sleep(Duration::from_millis(2)).await;
            let topic_name = format!("topic{i}");
            debug!("creating topic: {}", topic_name);
            let topic = DefaultTest::with_spec(topic_name, TestSpec::default()).with_context(3);
            let _ = topic_store.apply_changes(vec![LSUpdate::Mod(topic)]).await;
        }

        // wait for controller to sync
        sleep(Duration::from_millis(100)).await;
        shutdown.notify();
        sleep(Duration::from_millis(1)).await;

        //  assert_eq!(last_change.load(SeqCst), 4);
    }
    #[fluvio_future::test]
    async fn test_change_listener_non_blocking() {
        let mut timer = sleep(Duration::from_millis(5));
        let store = Arc::new(DefaultTestStore::default());
        let listener = store.change_listener();

        // no events, this should timeout
        select! {

            _ = listener.listen() => {
            panic!("test failed");
            },
            _ = &mut timer => {
                // test succeeds
            }

        }
    }

    #[test]
    fn test_wait_for_first_change_assumptions() {
        let topic_store = Arc::new(DefaultTestStore::default());

        // wait_for_first_change() requires that ChangeListener is initialized with current_change = 0
        assert_eq!(0, topic_store.change_listener().current_change())
    }

    #[fluvio_future::test]
    async fn test_change_listener() {
        let topic_store = Arc::new(DefaultTestStore::default());
        let last_change = Arc::new(AtomicI64::new(0));
        let shutdown = SimpleEvent::shared();
        let topic_name = "topic";
        let initial_topic = DefaultTest::with_spec(topic_name, TestSpec::default());
        let has_been_updated = Arc::new(AtomicBool::default());

        // Start a batch of listeners before changes have been.
        let jh = start_batch_of_test_listeners(topic_store.clone(), has_been_updated.clone());
        TestController::start(topic_store.clone(), shutdown.clone(), last_change.clone());

        // set flag that we are about to update, then do the update
        has_been_updated.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = topic_store.sync_all(vec![initial_topic.clone()]).await;

        // make sure that every listener got notified and returned
        for j in jh {
            j.await
        }

        // Test batch again with a store that already has updates
        let jh = start_batch_of_test_listeners(topic_store.clone(), has_been_updated.clone());
        // make sure that every listener got notified and returned
        for j in jh {
            j.await
        }

        // update with apply_changes
        let topic = DefaultTest::with_spec(topic_name, TestSpec::default());

        let _ = topic_store.apply_changes(vec![LSUpdate::Mod(topic)]).await;

        // Test batch again to make sure returns correctly after an apply_changes
        let jh = start_batch_of_test_listeners(topic_store, has_been_updated);
        // make sure that every listener got notified and returned
        for j in jh {
            j.await
        }

        // wait for controller to sync
        sleep(Duration::from_millis(100)).await;
        shutdown.notify();
        sleep(Duration::from_millis(1)).await;
    }

    fn start_batch_of_test_listeners(
        store: Arc<LocalStore<TestSpec, TestMeta>>,
        has_been_updated: Arc<AtomicBool>,
    ) -> Vec<JoinHandle<()>> {
        (0..10u32)
            // let jh: Vec<()> = (0..10u32)
            .map(|_| {
                let store = store.clone();

                spawn(listener_thread(store, has_been_updated.clone()))
            })
            .collect()
    }

    async fn listener_thread<S, C>(store: Arc<LocalStore<S, C>>, has_been_updated: Arc<AtomicBool>)
    where
        S: Spec,
        C: MetadataItem,
    {
        store.wait_for_first_change().await;
        // Make sure that we never return before we update the store
        assert!(has_been_updated.load(std::sync::atomic::Ordering::Relaxed));
    }
}
