use std::ops::Deref;
use std::ops::DerefMut;
use std::hash::Hash;
use std::hash::Hasher;
use std::cmp::Eq;
use std::cmp::PartialEq;
use std::fmt;

pub type Epoch = i64;

/// Keep track of changes to object using epoch
/// for every changes to objects, epoch counter must be incremented
#[derive(Debug, Default, Clone)]
pub struct EpochCounter<T> {
    epoch: Epoch,
    inner: T,
}

impl<T> Hash for EpochCounter<T>
where
    T: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl<T> PartialEq for EpochCounter<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T> Eq for EpochCounter<T> where T: Eq {}

impl<T> Deref for EpochCounter<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for EpochCounter<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> fmt::Display for EpochCounter<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "epoch: {}", self.epoch)
    }
}

impl<T> From<T> for EpochCounter<T> {
    fn from(inner: T) -> Self {
        Self { epoch: 0, inner }
    }
}

impl<T> EpochCounter<T> {
    pub fn new(inner: T) -> Self {
        Self { epoch: 0, inner }
    }

    pub fn new_with_epoch(inner: T, epoch: impl Into<i64>) -> Self {
        Self {
            epoch: epoch.into(),
            inner,
        }
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

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    fn set_epoch(&mut self, epoch: Epoch) {
        self.epoch = epoch;
    }

    pub fn increment(&mut self) {
        self.epoch += 1;
    }

    pub fn decrement(&mut self) {
        self.epoch -= 1;
    }
}

pub use old_map::*;

mod old_map {

    use std::collections::HashMap;
    use std::hash::Hash;
    use std::borrow::Borrow;

    use super::*;

    /// use epoch counter for every value in the hashmap
    /// if value are deleted, it is moved to thrash can (deleted)
    /// using epoch counter, level changes can be calculated
    #[derive(Debug, Default)]
    pub struct EpochMap<K, V> {
        epoch: EpochCounter<()>,
        fence: EpochCounter<()>, // last changes
        map: HashMap<K, EpochCounter<V>>,
        deleted: Vec<EpochCounter<V>>,
    }

    impl<K, V> Deref for EpochMap<K, V> {
        type Target = HashMap<K, EpochCounter<V>>;

        fn deref(&self) -> &Self::Target {
            &self.map
        }
    }

    impl<K, V> DerefMut for EpochMap<K, V> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.map
        }
    }

    impl<K, V> EpochMap<K, V> {
        pub fn increment_epoch(&mut self) {
            self.epoch.increment();
        }

        pub fn epoch(&self) -> Epoch {
            self.epoch.epoch()
        }

        /// fence history to current epoch,
        /// older before fence will be lost
        pub fn mark_fence(&mut self) {
            self.deleted = vec![];
            self.fence = self.epoch.clone();
        }
    }

    impl<K, V> EpochMap<K, V>
    where
        K: Eq + Hash,
    {
        pub fn new() -> Self {
            Self::new_with_map(HashMap::new())
        }

        pub fn new_with_map(map: HashMap<K, EpochCounter<V>>) -> Self {
            Self {
                epoch: EpochCounter::default(),
                fence: EpochCounter::default(),
                map,
                deleted: vec![],
            }
        }

        /// insert new value
        /// remove history from deleted set
        pub fn insert(&mut self, key: K, value: V) -> Option<EpochCounter<V>>
        where
            K: Clone,
        {
            let mut epoch_value: EpochCounter<V> = value.into();
            epoch_value.set_epoch(self.epoch.epoch());
            self.map.insert(key, epoch_value)
        }

        /// remove existing value
        /// if successful, remove are added to history
        pub fn remove<Q>(&mut self, k: &Q) -> Option<EpochCounter<V>>
        where
            K: Borrow<Q>,
            Q: ?Sized + Hash + Eq,
            V: Clone,
        {
            if let Some((_, mut old_value)) = self.map.remove_entry(k) {
                old_value.set_epoch(self.epoch.epoch());
                self.deleted.push(old_value.clone());
                Some(old_value)
            } else {
                None
            }
        }
    }

    impl<K, V> EpochMap<K, V>
    where
        K: Clone,
    {
        pub fn clone_keys(&self) -> Vec<K> {
            self.keys().cloned().collect()
        }
    }

    impl<K, V> EpochMap<K, V>
    where
        V: Clone,
        K: Clone,
    {
        pub fn clone_values(&self) -> Vec<V> {
            self.values().cloned().map(|c| c.inner_owned()).collect()
        }

        /// find all changes since a epoch
        /// if epoch is before fence, return full changes with epoch,
        /// otherwise return delta changes
        /// user should keep that epoch and do subsequent changes
        pub fn changes_since<E>(&self, epoch_value: E) -> EpochChanges<V>
        where
            Epoch: From<E>,
        {
            let epoch = epoch_value.into();
            if epoch < self.fence.epoch() {
                return EpochChanges {
                    epoch: self.epoch.epoch(),
                    changes: EpochDeltaChanges::SyncAll(self.clone_values()),
                };
            }

            if epoch == self.epoch() {
                return EpochChanges {
                    epoch: self.epoch.epoch(),
                    changes: EpochDeltaChanges::empty(),
                };
            }

            let updates = self
                .values()
                .filter_map(|v| {
                    if v.epoch > epoch {
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
                    if d.epoch > epoch {
                        Some(d.inner().clone())
                    } else {
                        None
                    }
                })
                .collect();

            EpochChanges {
                epoch: self.epoch.epoch(),
                changes: EpochDeltaChanges::Changes((updates, deletes)),
            }
        }
    }

    pub struct EpochChanges<V> {
        // current epoch
        pub epoch: Epoch,
        changes: EpochDeltaChanges<V>,
    }

    impl<V: fmt::Debug> fmt::Debug for EpochChanges<V> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.debug_struct("EpochChanges")
                .field("epoch", &self.epoch)
                .field("changes", &self.changes)
                .finish()
        }
    }

    impl<V> EpochChanges<V> {
        pub fn new(epoch: Epoch, changes: EpochDeltaChanges<V>) -> Self {
            Self { epoch, changes }
        }

        /// current epoch
        pub fn current_epoch(&self) -> &Epoch {
            &self.epoch
        }

        /// return all updates regardless of sync or changes
        /// (update,deletes)
        pub fn parts(self) -> (Vec<V>, Vec<V>) {
            match self.changes {
                EpochDeltaChanges::SyncAll(all) => (all, vec![]),
                EpochDeltaChanges::Changes(changes) => changes,
            }
        }

        pub fn is_empty(&self) -> bool {
            match &self.changes {
                EpochDeltaChanges::SyncAll(all) => all.is_empty(),
                EpochDeltaChanges::Changes(changes) => changes.0.is_empty() && changes.1.is_empty(),
            }
        }

        /// is change contain sync all
        pub fn is_sync_all(&self) -> bool {
            match &self.changes {
                EpochDeltaChanges::SyncAll(_) => true,
                EpochDeltaChanges::Changes(_) => false,
            }
        }
    }

    pub enum EpochDeltaChanges<V> {
        SyncAll(Vec<V>),
        Changes((Vec<V>, Vec<V>)),
    }

    impl<V> EpochDeltaChanges<V> {
        pub fn empty() -> Self {
            Self::Changes((vec![], vec![]))
        }
    }

    impl<V: fmt::Debug> fmt::Debug for EpochDeltaChanges<V> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::SyncAll(all) => f.debug_tuple("SyncAll").field(all).finish(),
                Self::Changes((add, del)) => {
                    f.debug_tuple("Changes").field(add).field(del).finish()
                }
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::fmt::Display;

    use serde::{Serialize, Deserialize};

    use crate::core::{Spec, Status};
    use crate::store::DefaultMetadataObject;

    use super::EpochMap;

    // define test spec and status
    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct TestSpec {
        replica: u16,
    }

    impl Spec for TestSpec {
        const LABEL: &'static str = "Test";
        type IndexKey = String;
        type Owner = Self;
        type Status = TestStatus;
    }

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct TestStatus {
        up: bool,
    }

    impl Status for TestStatus {}

    impl Display for TestStatus {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{self:?}")
        }
    }

    type DefaultTest = DefaultMetadataObject<TestSpec>;

    type TestEpochMap = EpochMap<String, DefaultTest>;

    #[test]
    fn test_epoch_map_empty() {
        let map = TestEpochMap::new();
        assert_eq!(map.epoch(), 0);
    }

    #[test]
    fn test_epoch_map_insert() {
        let mut map = TestEpochMap::new();

        // increase epoch
        // epoch must be increased before any write occur manually here
        // in the store, this is done automatically but this is low level interface
        map.increment_epoch();

        let test1 = DefaultTest::with_key("t1");
        map.insert(test1.key_owned(), test1);

        assert_eq!(map.epoch(), 1);

        // test with before base epoch
        {
            let changes = map.changes_since(-1);
            assert_eq!(*changes.current_epoch(), 1); // current epoch is 1
            assert!(changes.is_sync_all()); // this is only delta

            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);
        }

        // test with base epoch
        {
            let changes = map.changes_since(0);
            assert_eq!(*changes.current_epoch(), 1); // current epoch is 1
            assert!(!changes.is_sync_all()); // this is only delta

            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);
        }

        // test with current epoch which should return empty
        {
            let changes = map.changes_since(1);
            assert_eq!(*changes.current_epoch(), 1); // current epoch is 1
            assert!(!changes.is_sync_all()); // this is only delta
            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 0);
            assert_eq!(deletes.len(), 0);
        }
    }

    #[test]
    fn test_epoch_map_insert_update() {
        let mut map = TestEpochMap::new();

        let test1 = DefaultTest::with_key("t1");
        let test2 = test1.clone();
        let test3 = DefaultTest::with_key("t2");

        // first epoch
        map.increment_epoch();
        map.insert(test1.key_owned(), test1);
        map.insert(test3.key_owned(), test3);

        // second epoch
        map.increment_epoch();
        map.insert(test2.key_owned(), test2);

        assert_eq!(map.epoch(), 2);

        // test with base epoch, this should return a single changes, both insert/update are consolidated into a single
        {
            let changes = map.changes_since(0);
            assert_eq!(*changes.current_epoch(), 2);
            assert!(!changes.is_sync_all());

            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 2);
            assert_eq!(deletes.len(), 0);
        }

        // test with middle epoch, this should still return a single changes
        {
            let changes = map.changes_since(1);
            assert_eq!(*changes.current_epoch(), 2);
            assert!(!changes.is_sync_all());
            let (updates, deletes) = changes.parts();
            assert_eq!(updates.len(), 1);
            assert_eq!(deletes.len(), 0);
        }
    }
}
