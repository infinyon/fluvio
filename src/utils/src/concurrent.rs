use std::collections::HashMap;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;


/// inefficient but simple concurren hashma
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

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let mut lock = self.write();
        lock.insert(key, value)
    }

    pub fn read(&self) -> RwLockReadGuard<HashMap<K, V>> {
        self.0.read().unwrap()
    }

    pub fn write(&self) -> RwLockWriteGuard<HashMap<K, V>> {
        self.0.write().unwrap()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.read().contains_key(key)
    }
}

#[derive(Debug)]
pub struct SimpleConcurrentBTreeMap<K,V>(RwLock<BTreeMap<K,V>>);

impl <K,V>Default for SimpleConcurrentBTreeMap<K,V> where K: Ord{

    fn default() -> Self {
         SimpleConcurrentBTreeMap(RwLock::new(BTreeMap::new()))
    }
   
}

impl <K,V> SimpleConcurrentBTreeMap<K,V> 
    where K: Ord 
{
    pub fn new() -> Self {
        SimpleConcurrentBTreeMap(RwLock::new(BTreeMap::new()))
    }

    pub fn read(&self) -> RwLockReadGuard<BTreeMap<K, V>> {
        self.0.read().unwrap()
    }

    pub fn write(&self) -> RwLockWriteGuard<BTreeMap<K, V>> {
        self.0.write().unwrap()
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let mut lock = self.write();
        lock.insert(key, value)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.read().contains_key(key)
    }


}