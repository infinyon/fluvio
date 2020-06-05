use std::sync::Arc;
use std::fmt::Debug;
use std::fmt::Display;
use std::borrow::Borrow;
use std::io::Error as IoError;
use std::io::ErrorKind;

use flv_util::SimpleConcurrentBTreeMap;

use crate::core::Spec;

use super::KVObject;

/// Local state in memory
#[derive(Debug)]
pub struct LocalStore<S>(SimpleConcurrentBTreeMap<S::Key, KVObject<S>>)
where
    S: Spec,
    <S as Spec>::Status: Debug;

impl<S> Default for LocalStore<S>
where
    S: Spec,
    <S as Spec>::Status: Debug,
{
    fn default() -> Self {
        LocalStore(SimpleConcurrentBTreeMap::new())
    }
}

impl<S> ::std::cmp::PartialEq for LocalStore<S>
where
    S: Spec + PartialEq,
    S::Status: PartialEq + Debug,
{
    fn eq(&self, other: &LocalStore<S>) -> bool {
        for (key, val) in self.0.read().iter() {
            let other_list = other.0.read();
            let other_val = match other_list.get(key) {
                Some(val) => val,
                None => return false,
            };
            if val != other_val {
                return false;
            }
        }
        true
    }
}

impl<S> LocalStore<S>
where
    S: Spec,
    S::Status: Debug,
{
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inner_store(&self) -> &SimpleConcurrentBTreeMap<S::Key, KVObject<S>> {
        &self.0
    }

    pub fn insert(&self, value: KVObject<S>) -> Option<KVObject<S>> {
        self.inner_store().write().insert(value.key_owned(), value)
    }

    /// visit all values
    pub fn visit_values<F>(&self, func: F)
    where
        F: FnMut(&'_ KVObject<S>),
    {
        self.inner_store().read().values().for_each(func);
    }
}

impl<S> LocalStore<S>
where
    S: Spec,
    S::Key: Ord + Clone,
    S::Status: Debug,
{
    /*
    pub fn delete<S>(&self, name: S) where S: AsRef<K> {
        self.inner_store().write().remove(name.as_ref());
    }
    */

    pub fn delete<K: ?Sized>(&self, key: &K)
    where
        S::Key: Borrow<K>,
        K: Ord,
    {
        self.inner_store().write().remove(key);
    }

    /// get copy of the value ref by key
    pub fn value<K: ?Sized>(&self, key: &K) -> Option<KVObject<S>>
    where
        S::Key: Borrow<K>,
        K: Ord,
    {
        match self.inner_store().read().get(key) {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }

    /// get copy of the spec ref by key
    pub fn spec<K: ?Sized>(&self, key: &K) -> Option<S>
    where
        S::Key: Borrow<K>,
        K: Ord,
    {
        match self.inner_store().read().get(key) {
            Some(value) => Some(value.spec.clone()),
            None => None,
        }
    }

    pub fn find_and_do<K, F>(&self, key: &K, mut func: F) -> Option<()>
    where
        F: FnMut(&'_ KVObject<S>),
        K: Ord,
        S::Key: Borrow<K>,
    {
        if let Some(value) = self.inner_store().read().get(key) {
            func(value);
            Some(())
        } else {
            None
        }
    }

    pub fn contains_key<K: ?Sized>(&self, key: &K) -> bool
    where
        S::Key: Borrow<K>,
        K: Ord,
    {
        self.inner_store().read().contains_key(key)
    }

    pub fn remove<K: ?Sized>(&self, key: &K) -> Option<KVObject<S>>
    where
        S::Key: Borrow<K>,
        K: Ord,
    {
        self.inner_store().write().remove(key)
    }

    pub fn count(&self) -> i32 {
        self.inner_store().read().len() as i32
    }

    pub fn all_keys(&self) -> Vec<S::Key> {
        self.inner_store().read().keys().cloned().collect()
    }

    pub fn all_values(&self) -> Vec<KVObject<S>> {
        self.inner_store().read().values().cloned().collect()
    }

    pub fn all_specs(&self) -> Vec<S> {
        self.inner_store()
            .read()
            .values()
            .map(|kv| kv.spec.clone())
            .collect()
    }

    /// update status
    pub fn update_status<K: ?Sized>(&self, key: &K, status: S::Status) -> Result<(), IoError>
    where
        S::Key: Borrow<K>,
        K: Display + Ord,
    {
        if let Some(old_kv) = self.inner_store().write().get_mut(key) {
            old_kv.status = status;
            Ok(())
        } else {
            Err(IoError::new(
                ErrorKind::InvalidData,
                format!("{} '{}': not found, cannot update", S::LABEL, key),
            ))
        }
    }
}

impl<S> Display for LocalStore<S>
where
    S: Spec,
    <S as Spec>::Status: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} Store count: {}",
            S::LABEL,
            self.inner_store().read().len()
        )
    }
}
