use std::ops::Deref;
use std::ops::DerefMut;
use std::hash::Hash;
use std::cmp::Eq;
use std::collections::HashMap;
use std::borrow::Borrow;

use super::EpochCounter;
use super::Epoch;
use super::EpochDeltaChanges;
use super::EpochChanges;

pub trait DualDiff {
    /// find dual diff
    fn diff(&self, another: &Self) -> MetadataChange;
}

/// What has been changed between two metadata
pub struct MetadataChange {
    pub spec: bool,
    pub status: bool,
}

impl MetadataChange {
    /// create change that change both spec and status
    fn full_change() -> Self {
        Self {
            spec: true,
            status: true,
        }
    }

    /// create no changes
    fn no_change() -> Self {
        Self {
            spec: false,
            status: false
        }
    }

    pub fn has_full_change(&self) -> bool {
        self.spec && self.status
    }

    /// check if there were any changes
    pub fn has_no_changes(&self) -> bool {
        !self.spec && !self.status
    }
}

#[derive(Debug, Default, Clone)]
pub struct DualEpochCounter<T> {
    spec_epoch: Epoch,
    status_epoch: Epoch,
    inner: T,
}

impl<T> DualEpochCounter<T> {
    pub fn new(inner: T) -> Self {
        Self {
            spec_epoch: 0,
            status_epoch: 0,
            inner,
        }
    }

    fn set_epoch(&mut self, epoch: Epoch) {
        self.spec_epoch = epoch;
        self.status_epoch = epoch;
    }

    fn set_spec_epoch(&mut self, epoch: Epoch) {
        self.spec_epoch = epoch;
    }

    fn set_status_epoch(&mut self, epoch: Epoch) {
        self.status_epoch = epoch;
    }

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

impl<K, V> DualEpochMap<K, V>
where
    V: DualDiff,
    K: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            epoch: EpochCounter::default(),
            fence: EpochCounter::default(),
            values: HashMap::new(),
            deleted: vec![],
        }
    }

    pub fn increment_epoch(&mut self) {
        self.epoch.increment();
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch.epoch()
    }

    /// updates the metadata if it is different from existing value
    pub fn update(&mut self, key: K, new_value: V) -> MetadataChange
    where
        K: Clone,
    {
        let mut new_value = DualEpochCounter::new(new_value);
        let current_epoch = self.epoch.epoch();

        // check each spec and status
        if let Some(existing_value) = self.values.get(&key) {
            let diff = existing_value.diff(new_value.inner());
            if diff.has_full_change() {
                new_value.set_epoch(current_epoch);
                self.values.insert(key, new_value);
            } else if diff.spec {
                new_value.set_spec_epoch(current_epoch);
                self.values.insert(key, new_value);
            } else if diff.status {
                new_value.set_status_epoch(current_epoch);
                self.values.insert(key, new_value);
            }
            diff
        } else {
            // doesn't exist, so this is new
            new_value.set_epoch(current_epoch);
            self.values.insert(key, new_value);
            MetadataChange::full_change()
        }
    }

    /// forcefully replace existing value
    pub fn insert(&mut self, key: K, value: V) -> Option<DualEpochCounter<V>> {
        let mut epoch_value = DualEpochCounter::new(value);
        epoch_value.set_epoch(self.epoch.epoch());
        self.values.insert(key, epoch_value)
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

    /// find all spec changes
    /// if epoch is before fence, return full changes with epoch,
    /// otherwise return delta changes
    /// user should keep that epoch and do subsequent changes
    pub fn spec_changes_since<E>(&self, epoch_value: E) -> EpochChanges<V>
    where
        Epoch: From<E>,
    {
        let epoch = epoch_value.into();
        if epoch < self.fence.epoch() {
            return EpochChanges::new(
                self.epoch.epoch(),
                EpochDeltaChanges::SyncAll(self.clone_values()),
            );
        }

        let updates = self
            .values()
            .filter_map(|v| {
                if v.spec_epoch > epoch {
                    Some(v.inner().clone())
                } else {
                    None
                }
            })
            .collect();

        let deletes = self
            .deleted
            .iter()
            .filter_map(|d| {
                if d.spec_epoch > epoch {
                    Some(d.inner().clone())
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

    /// find all status changes
    pub fn status_changes_since<E>(&self, epoch_value: E) -> EpochChanges<V>
    where
        Epoch: From<E>,
    {
        let epoch = epoch_value.into();
        if epoch < self.fence.epoch() {
            return EpochChanges::new(
                self.epoch.epoch(),
        EpochDeltaChanges::SyncAll(self.clone_values())
            )
        }

        let updates = self
            .values()
            .filter_map(|v| {
                if v.status_epoch > epoch {
                    Some(v.inner().clone())
                } else {
                    None
                }
            })
            .collect();

        let deletes = self
            .deleted
            .iter()
            .filter_map(|d| {
                if d.status_epoch > epoch {
                    Some(d.inner().clone())
                } else {
                    None
                }
            })
            .collect();

        EpochChanges::new(
            self.epoch.epoch(),
    EpochDeltaChanges::Changes((updates, deletes))
        )
    }
}

#[cfg(test)]
mod test {

    use crate::core::{Spec, Status};
    use crate::store::DefaultMetadataObject;

    use super::DualEpochMap;
    use super::MetadataChange;

    // define test spec and status
    #[derive(Debug, Default, Clone, PartialEq)]
    struct TestSpec {
        replica: u16,
    }

    impl Spec for TestSpec {
        const LABEL: &'static str = "Test";
        type IndexKey = String;
        type Owner = Self;
        type Status = TestStatus;
    }

    #[derive(Debug, Default, Clone, PartialEq)]
    struct TestStatus {
        up: bool,
    }

    impl Status for TestStatus {}

    type DefaultTest = DefaultMetadataObject<TestSpec>;

    type TestEpochMap = DualEpochMap<String, DefaultTest>;

    #[test]
    fn test_metadata_changes() {
        let full_change = MetadataChange::full_change();
        assert!(full_change.has_full_change());
        assert!(!full_change.has_no_changes());
        let no_change = MetadataChange::no_change();
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
        map.update(test1.key_owned(), test1);

        assert_eq!(map.epoch(), 1);

        // test with before base epoch
        {
            let spec_changes = map.spec_changes_since(-1);
            assert_eq!(*spec_changes.current_epoch(), 1); // current epoch is 1
            assert!(spec_changes.is_sync_all()); // this is only delta

            let (updates, deletes) = spec_changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);


            let status_changes = map.status_changes_since(-1);
            assert_eq!(*status_changes.current_epoch(), 1); // current epoch is 1
            assert!(status_changes.is_sync_all()); // this is only delta

            let (updates2, deletes2) = status_changes.parts();
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
        }

        
        // test with current epoch which should return empty
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
        }
    }



    #[test]
    fn test_epoch_map_update_multiple() {
        let mut map = TestEpochMap::new();

        let test1 = DefaultTest::with_key("t1");
        let mut test2 = test1.clone();
        test2.status.up = true;

        // first epoch
        map.increment_epoch();

        assert!(map.update(test1.key_owned(), test1).has_full_change());

        map.increment_epoch();
        let changes = map.update(test2.key_owned(), test2);
        assert!(!changes.spec);
        assert!(changes.status);


        // update the 
        assert_eq!(map.epoch(), 2);

        
        // test with base epoch, this should return a single changes, both insert/update are consolidated into a single
        {
            let changes = map.spec_changes_since(0);
            assert_eq!(*changes.current_epoch(), 2);
            assert!(!changes.is_sync_all());

          //  let (updates, deletes) = changes.parts();
          //  assert_eq!(updates.len(), 1);
          //  assert_eq!(deletes.len(), 0);
        }
        

        /*
        // test with middle epoch, this should still return a single changes
        {
            let changes = map.spec_changes_since(1);
            assert_eq!(*changes.current_epoch(), 2);
            assert!(!changes.is_sync_all());
            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);
        }
        */
    }
}
