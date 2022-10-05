use std::ops::Deref;
use std::ops::DerefMut;
use std::hash::Hash;
use std::cmp::Eq;
use std::collections::HashMap;
use std::borrow::Borrow;

use once_cell::sync::Lazy;
use tracing::trace;

use super::EpochCounter;
use super::Epoch;
use super::EpochDeltaChanges;
use super::EpochChanges;

pub trait DualDiff {
    /// check if another is different from myself
    fn diff(&self, new_value: &Self) -> ChangeFlag;
}

pub static FULL_FILTER: Lazy<ChangeFlag> = Lazy::new(ChangeFlag::all);

pub static SPEC_FILTER: Lazy<ChangeFlag> = Lazy::new(|| ChangeFlag {
    spec: true,
    status: false,
    meta: false,
});

pub static STATUS_FILTER: Lazy<ChangeFlag> = Lazy::new(|| ChangeFlag {
    spec: false,
    status: true,
    meta: false,
});
pub static META_FILTER: Lazy<ChangeFlag> = Lazy::new(|| ChangeFlag {
    spec: false,
    status: false,
    meta: true,
});

/// Filter for metadata change
#[derive(Debug)]
pub struct ChangeFlag {
    pub spec: bool,
    pub status: bool,
    pub meta: bool,
}

impl ChangeFlag {
    pub fn all() -> Self {
        Self {
            spec: true,
            status: true,
            meta: true,
        }
    }

    /// create no changes
    #[inline]
    pub fn no_change() -> Self {
        Self {
            spec: false,
            status: false,
            meta: false,
        }
    }

    #[inline]
    pub fn has_full_change(&self) -> bool {
        self.spec && self.status && self.meta
    }

    /// check if there were any changes
    #[inline]
    pub fn has_no_changes(&self) -> bool {
        !self.spec && !self.status && !self.meta
    }
}

/// Keep track of internal changes to object
/// Track 3 different changes (spec,status,meta)
#[derive(Debug, Default, Clone)]
pub struct DualEpochCounter<T> {
    spec_epoch: Epoch,
    status_epoch: Epoch,
    meta_epoch: Epoch,
    inner: T,
}

impl<T> DualEpochCounter<T> {
    pub fn new(inner: T) -> Self {
        Self {
            spec_epoch: 0,
            status_epoch: 0,
            meta_epoch: 0,
            inner,
        }
    }

    /// set epoch
    fn set_epoch(&mut self, epoch: Epoch) {
        self.spec_epoch = epoch;
        self.status_epoch = epoch;
        self.meta_epoch = epoch;
    }

    // copy epoch values from old value
    fn copy_epoch(&mut self, old: &Self) {
        self.spec_epoch = old.spec_epoch;
        self.status_epoch = old.status_epoch;
        self.meta_epoch = old.meta_epoch;
    }

    #[inline]
    pub fn spec_epoch(&self) -> Epoch {
        self.spec_epoch
    }

    fn set_spec_epoch(&mut self, epoch: Epoch) {
        self.spec_epoch = epoch;
    }

    #[inline]
    pub fn status_epoch(&self) -> Epoch {
        self.status_epoch
    }

    fn set_status_epoch(&mut self, epoch: Epoch) {
        self.status_epoch = epoch;
    }

    #[inline]
    pub fn meta_epoch(&self) -> Epoch {
        self.meta_epoch
    }

    fn set_meta_epoch(&mut self, epoch: Epoch) {
        self.meta_epoch = epoch;
    }

    #[inline]
    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn inner_owned(self) -> T {
        self.inner
    }
}

impl<T> Deref for DualEpochCounter<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for DualEpochCounter<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> From<T> for DualEpochCounter<T> {
    fn from(inner: T) -> Self {
        Self::new(inner)
    }
}

/// Epoch Map with separate mapping
#[derive(Debug, Default)]
pub struct DualEpochMap<K, V> {
    epoch: EpochCounter<()>,
    fence: EpochCounter<()>, // last changes
    values: HashMap<K, DualEpochCounter<V>>,
    deleted: Vec<DualEpochCounter<V>>,
}

impl<K, V> Deref for DualEpochMap<K, V> {
    type Target = HashMap<K, DualEpochCounter<V>>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<K, V> DerefMut for DualEpochMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}

impl<K, V> DualEpochMap<K, V> {
    pub fn increment_epoch(&mut self) {
        self.epoch.increment();
    }

    pub fn decrement_epoch(&mut self) {
        self.epoch.decrement();
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch.epoch()
    }
}

impl<K, V> DualEpochMap<K, V>
where
    V: DualDiff,
    K: Eq + Hash,
{
    pub fn new() -> Self {
        Self::new_with_map(HashMap::new())
    }

    pub fn new_with_map(values: HashMap<K, DualEpochCounter<V>>) -> Self {
        Self {
            epoch: EpochCounter::default(),
            fence: EpochCounter::default(),
            values,
            deleted: vec![],
        }
    }

    /// updates the metadata if it is different from existing value
    //  if this return some then it means replace
    //  otherwise change occurred
    pub fn update(&mut self, key: K, new_value: V) -> Option<ChangeFlag>
    where
        K: Clone,
    {
        let mut new_value = DualEpochCounter::new(new_value);
        let current_epoch = self.epoch.epoch();

        trace!(current_epoch, "updating");

        // check each spec and status
        if let Some(existing_value) = self.values.get_mut(&key) {
            let diff = existing_value.diff(new_value.inner());
            trace!("existing diff: {:#?}", diff);
            if !diff.has_no_changes() {
                new_value.copy_epoch(existing_value);
                if diff.spec {
                    new_value.set_spec_epoch(current_epoch);
                }
                if diff.status {
                    new_value.set_status_epoch(current_epoch);
                }
                if diff.meta {
                    new_value.set_meta_epoch(current_epoch);
                }

                *existing_value = new_value;
            }

            Some(diff)
        } else {
            // doesn't exist, so everything is new
            new_value.set_epoch(current_epoch);
            self.values.insert(key, new_value);
            None
        }
    }

    /// remove existing value
    /// if successful, remove are added to history
    pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<DualEpochCounter<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
        V: Clone,
    {
        if let Some((_, mut old_value)) = self.values.remove_entry(k) {
            old_value.set_epoch(self.epoch.epoch());
            self.deleted.push(old_value.clone());
            Some(old_value)
        } else {
            None
        }
    }

    /// fence history to current epoch,
    /// older before fence will be lost
    pub fn mark_fence(&mut self) {
        self.deleted = vec![];
        self.fence = self.epoch.clone();
    }
}

impl<K, V> DualEpochMap<K, V>
where
    K: Clone,
{
    pub fn clone_keys(&self) -> Vec<K> {
        self.keys().cloned().collect()
    }
}

impl<K, V> DualEpochMap<K, V>
where
    V: Clone,
    K: Clone,
{
    pub fn clone_values(&self) -> Vec<V> {
        self.values().cloned().map(|c| c.inner_owned()).collect()
    }

    /// find all spec changes given epoch
    /// if epoch is before fence, return full changes with epoch,
    /// otherwise return delta changes
    /// user should keep that epoch and do subsequent changes
    pub fn spec_changes_since<E>(&self, epoch_value: E) -> EpochChanges<V>
    where
        Epoch: From<E>,
    {
        let epoch = epoch_value.into();
        self.changes_since_with_filter(epoch, &SPEC_FILTER)
    }

    /// find all status changes
    pub fn status_changes_since<E>(&self, epoch_value: E) -> EpochChanges<V>
    where
        Epoch: From<E>,
    {
        let epoch = epoch_value.into();
        self.changes_since_with_filter(epoch, &STATUS_FILTER)
    }

    pub fn meta_changes_since<E>(&self, epoch_value: E) -> EpochChanges<V>
    where
        Epoch: From<E>,
    {
        let epoch = epoch_value.into();
        self.changes_since_with_filter(epoch, &META_FILTER)
    }

    /// all changes (spec and status) since epoch
    pub fn changes_since<E>(&self, epoch_value: E) -> EpochChanges<V>
    where
        Epoch: From<E>,
    {
        let epoch = epoch_value.into();

        self.changes_since_with_filter(epoch, &FULL_FILTER)
    }

    /// find all status changes, only updates are accounted for
    pub fn changes_since_with_filter(&self, epoch: Epoch, filter: &ChangeFlag) -> EpochChanges<V> {
        if epoch < self.fence.epoch() {
            return EpochChanges::new(
                self.epoch.epoch(),
                EpochDeltaChanges::SyncAll(self.clone_values()),
            );
        }

        if epoch == self.epoch() {
            return EpochChanges::new(self.epoch.epoch(), EpochDeltaChanges::empty());
        }

        let updates: Vec<V> = self
            .values()
            .filter_map(|v| {
                if filter.spec && v.spec_epoch > epoch
                    || filter.status && v.status_epoch > epoch
                    || filter.meta && v.meta_epoch > epoch
                {
                    Some(v.inner().clone())
                } else {
                    None
                }
            })
            .collect();

        let deletes = self
            .deleted
            .iter()
            .filter_map(|v| {
                if filter.spec && v.spec_epoch > epoch
                    || filter.status && v.status_epoch > epoch
                    || filter.meta && v.meta_epoch > epoch
                {
                    Some(v.inner().clone())
                } else {
                    None
                }
            })
            .collect();

        EpochChanges::new(
            self.epoch.epoch(),
            EpochDeltaChanges::Changes((updates, deletes)),
        )
    }
}

#[cfg(test)]
mod test {

    use crate::fixture::{DefaultTest, TestEpochMap};

    use super::ChangeFlag;

    #[test]
    fn test_metadata_changes() {
        let full_change = ChangeFlag::all();
        assert!(full_change.has_full_change());
        assert!(!full_change.has_no_changes());
        let no_change = ChangeFlag::no_change();
        assert!(no_change.has_no_changes());
        assert!(!no_change.has_full_change());
    }

    #[test]
    fn test_epoch_map_empty() {
        let map = TestEpochMap::new();
        assert_eq!(map.epoch(), 0);
    }

    #[test]
    fn test_epoch_map_update_simple() {
        let mut map = TestEpochMap::new();

        // increase epoch
        // epoch must be increased before any write occur manually here
        // in the store, this is done automatically but this is low level interface
        map.increment_epoch();

        let test1 = DefaultTest::with_key("t1");
        assert!(map.update(test1.key_owned(), test1).is_none()); // new

        assert_eq!(map.epoch(), 1);

        // test with before base epoch
        {
            let spec_changes = map.spec_changes_since(-1);
            assert_eq!(*spec_changes.current_epoch(), 1); // current epoch is 1
            assert!(spec_changes.is_sync_all());
            let (updates, deletes) = spec_changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let status_changes = map.status_changes_since(-1);
            assert_eq!(*status_changes.current_epoch(), 1); // current epoch is 1
            assert!(status_changes.is_sync_all());
            let (updates2, deletes2) = status_changes.parts();
            assert_eq!(updates2.len(), 1);
            assert_eq!(deletes2.len(), 0);

            let meta_changes = map.meta_changes_since(-1);
            assert_eq!(*meta_changes.current_epoch(), 1); // current epoch is 1
            assert!(meta_changes.is_sync_all());
            let (updates2, deletes2) = meta_changes.parts();
            assert_eq!(updates2.len(), 1);
            assert_eq!(deletes2.len(), 0);

            let any_change = map.changes_since(-1);
            assert_eq!(*any_change.current_epoch(), 1);
            assert!(any_change.is_sync_all());
            let (updates2, deletes2) = any_change.parts();
            assert_eq!(updates2.len(), 1);
            assert_eq!(deletes2.len(), 0);
        }

        // test with current epoch, this return just 1 changes
        {
            let spec_changes = map.spec_changes_since(0);
            assert_eq!(*spec_changes.current_epoch(), 1); // current epoch is 1
            assert!(!spec_changes.is_sync_all()); // this is only delta
            let (updates, deletes) = spec_changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let status_changes = map.status_changes_since(0);
            assert_eq!(*status_changes.current_epoch(), 1); // current epoch is 1
            assert!(!status_changes.is_sync_all()); // this is only delta
            let (updates, deletes) = status_changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let meta_changes = map.meta_changes_since(0);
            assert_eq!(*meta_changes.current_epoch(), 1); // current epoch is 1
            assert!(!meta_changes.is_sync_all()); // this is only delta
            let (updates, deletes) = meta_changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let any_change = map.changes_since(0);
            assert_eq!(*any_change.current_epoch(), 1);
            assert!(!any_change.is_sync_all());
            let (updates2, deletes2) = any_change.parts();
            assert_eq!(updates2.len(), 1);
            assert_eq!(deletes2.len(), 0);
        }

        // test with current epoch which should return 1 single change as well
        {
            let spec_changes = map.spec_changes_since(1);
            assert_eq!(*spec_changes.current_epoch(), 1); // current epoch is 1
            assert!(!spec_changes.is_sync_all()); // this is only delta
            let (updates, deletes) = spec_changes.parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let status_changes = map.status_changes_since(1);
            assert_eq!(*status_changes.current_epoch(), 1); // current epoch is 1
            assert!(!status_changes.is_sync_all()); // this is only delta
            let (updates, deletes) = status_changes.parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let meta_changes = map.meta_changes_since(1);
            assert_eq!(*meta_changes.current_epoch(), 1); // current epoch is 1
            assert!(!meta_changes.is_sync_all()); // this is only delta
            let (updates, deletes) = meta_changes.parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let any_change = map.changes_since(1);
            assert_eq!(*any_change.current_epoch(), 1);
            assert!(!any_change.is_sync_all());
            let (updates2, deletes2) = any_change.parts();
            assert_eq!(updates2.len(), 0);
            assert_eq!(deletes2.len(), 0);
        }
    }

    #[test]
    fn test_epoch_map_update_status() {
        let mut map = TestEpochMap::new();

        let test1 = DefaultTest::with_key("t1");
        let mut test2 = test1.clone();
        test2.status.up = true;

        // first epoch
        map.increment_epoch();

        assert_eq!(test1.ctx().item().rev, 0);
        assert!(map.update(test1.key_owned(), test1).is_none());

        map.increment_epoch();

        // only update status
        let changes = map
            .update(test2.key_owned(), test2.next_rev())
            .expect("update");
        assert!(!changes.spec);
        assert!(changes.status);

        // update the
        assert_eq!(map.epoch(), 2);

        // test with base epoch, this should return a single changes for spec and status
        {
            let (updates, deletes) = map.spec_changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.status_changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.meta_changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);
        }

        // test with middle epoch, this should just return status

        {
            let (updates, deletes) = map.spec_changes_since(1).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.status_changes_since(1).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.changes_since(1).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.meta_changes_since(1).parts();
            assert_eq!(updates.len(), 1); // rev has changed
            assert_eq!(deletes.len(), 0);
        }

        {
            let (updates, deletes) = map.spec_changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.status_changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.meta_changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);
        }
    }

    #[test]
    fn test_epoch_map_update_spec() {
        let mut map = TestEpochMap::new();

        let test1 = DefaultTest::with_key("t1");
        let mut test2 = test1.clone();
        test2.spec.replica = 20;

        // first epoch
        map.increment_epoch();

        // there is no test 1 prior, so update should not occur
        assert!(map.update(test1.key_owned(), test1).is_none());

        map.increment_epoch();
        let changes = map
            .update(test2.key_owned(), test2.next_rev())
            .expect("update");
        assert!(changes.spec);
        assert!(!changes.status);

        // update the
        assert_eq!(map.epoch(), 2);

        // test with base epoch, this should return a single changes for spec and status
        {
            let (updates, deletes) = map.spec_changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.status_changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.meta_changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);
        }

        // test with middle epoch, this should just return status

        {
            let (updates, deletes) = map.spec_changes_since(1).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.status_changes_since(1).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.changes_since(1).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.meta_changes_since(1).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);
        }

        {
            let (updates, deletes) = map.spec_changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.status_changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.meta_changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);
        }
    }

    #[test]
    fn test_epoch_map_update_meta() {
        let mut map = TestEpochMap::new();

        let test1 = DefaultTest::with_key("t1");
        let mut test2 = test1.clone();
        test2.ctx.item_mut().comment = "test".to_owned();

        map.increment_epoch();

        assert!(map.update(test1.key_owned(), test1).is_none());

        // without rev update, no updates
        assert!(map
            .update(test2.key_owned(), test2.clone())
            .expect("update")
            .has_no_changes());

        map.increment_epoch();
        let changes = map
            .update(test2.key_owned(), test2.next_rev())
            .expect("update");
        assert!(!changes.spec);
        assert!(!changes.status);
        assert!(changes.meta);

        // update the
        assert_eq!(map.epoch(), 2);

        // test with base epoch, this should return a single changes for spec and status
        {
            let (updates, deletes) = map.spec_changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.status_changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.meta_changes_since(0).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);
        }

        // changes with meta changes only

        {
            let (updates, deletes) = map.spec_changes_since(1).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.status_changes_since(1).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.changes_since(1).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.meta_changes_since(1).parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);
        }

        {
            let (updates, deletes) = map.spec_changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.status_changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);

            let (updates, deletes) = map.meta_changes_since(2).parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);
        }
    }
}
