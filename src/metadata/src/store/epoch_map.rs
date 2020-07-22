use std::ops::Deref;
use std::ops::DerefMut;
use std::hash::Hash;
use std::hash::Hasher;
use std::cmp::Eq;
use std::cmp::PartialEq;

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

use std::fmt;

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

    pub fn new_with_epoch<E>(inner: T, epoch: E) -> Self
    where
        E: Into<i64>,
    {
        Self {
            epoch: epoch.into(),
            inner,
        }
    }

    pub fn inner(&self) -> &T {
        &self.inner
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

        pub fn increment_epoch(&mut self) {
            self.epoch.increment();
        }

        pub fn epoch(&self) -> Epoch {
            self.epoch.epoch()
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
        pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<EpochCounter<V>>
        where
            K: Borrow<Q>,
            Q: Hash + Eq,
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

        /// fence history to current epoch,
        /// older before fence will be lost
        pub fn mark_fence(&mut self) {
            self.deleted = vec![];
            self.fence = self.epoch.clone();
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
        pub epoch: Epoch,
        changes: EpochDeltaChanges<V>,
    }

    impl<V> EpochChanges<V> {
        /// return all updates regardless of sync or changes
        /// (update,deletes)
        pub fn parts(self) -> (Vec<V>, Vec<V>) {
            match self.changes {
                EpochDeltaChanges::SyncAll(all) => (all, vec![]),
                EpochDeltaChanges::Changes(changes) => changes,
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
}
