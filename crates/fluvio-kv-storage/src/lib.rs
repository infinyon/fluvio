mod log;

use std::hash::Hash;

use anyhow::Result;
use fluvio_protocol::{Encoder, Decoder};

pub use log::{LogBasedKVStorage, Log};

#[allow(async_fn_in_trait)]
pub trait KVStorage<K, V>
where
    K: Encoder + Decoder + Hash + Eq + Clone,
    V: Encoder + Decoder + Clone,
{
    async fn get(&self, key: &K) -> Result<Option<V>>;

    async fn delete(&mut self, key: &K) -> Result<()>;

    async fn put(&mut self, key: impl Into<K>, value: impl Into<V>) -> Result<()>;

    async fn entries(&self) -> Result<Vec<(K, V)>>;
}
