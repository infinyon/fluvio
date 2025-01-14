use std::collections::HashMap;
use std::collections::BTreeMap;
use std::hash::Hash;
use async_lock::RwLock;
use async_lock::RwLockReadGuard;
use async_lock::RwLockWriteGuard;

/// inefficient but simple concurrent hashmap
/// this should be only used in a test
/// it locks for every write
pub struct SimpleConcurrentHashMap<K, V>(RwLock<HashMap<K, V>>);

impl<K, V> SimpleConcurrentHashMap<K, V>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        SimpleConcurrentHashMap(RwLock::new(HashMap::new()))
    }

    pub async fn insert(&self, key: K, value: V) -> Option<V> {
        let mut lock = self.write().await;
        lock.insert(key, value)
    }

    pub async fn read(&'_ self) -> RwLockReadGuard<'_, HashMap<K, V>> {
        self.0.read().await
    }

    pub async fn write(&'_ self) -> RwLockWriteGuard<'_, HashMap<K, V>> {
        self.0.write().await
    }

    pub async fn contains_key(&self, key: &K) -> bool {
        self.read().await.contains_key(key)
    }
}

impl<K, V> Default for SimpleConcurrentHashMap<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct SimpleConcurrentBTreeMap<K, V>(RwLock<BTreeMap<K, V>>);

impl<K, V> Default for SimpleConcurrentBTreeMap<K, V>
where
    K: Ord,
{
    fn default() -> Self {
        SimpleConcurrentBTreeMap(RwLock::new(BTreeMap::new()))
    }
}

impl<K, V> SimpleConcurrentBTreeMap<K, V>
where
    K: Ord,
{
    pub fn new() -> Self {
        Self(RwLock::new(BTreeMap::new()))
    }

    pub fn new_with_map(map: BTreeMap<K, V>) -> Self {
        Self(RwLock::new(map))
    }

    pub async fn read(&'_ self) -> RwLockReadGuard<'_, BTreeMap<K, V>> {
        self.0.read().await
    }

    pub async fn write(&'_ self) -> RwLockWriteGuard<'_, BTreeMap<K, V>> {
        self.0.write().await
    }

    pub fn try_write(&self) -> Option<RwLockWriteGuard<BTreeMap<K, V>>> {
        self.0.try_write()
    }

    pub async fn insert(&self, key: K, value: V) -> Option<V> {
        let mut lock = self.write().await;
        lock.insert(key, value)
    }

    pub async fn contains_key(&self, key: &K) -> bool {
        self.read().await.contains_key(key)
    }
}
