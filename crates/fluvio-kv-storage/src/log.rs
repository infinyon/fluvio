use std::io::Cursor;
use std::hash::Hash;
use std::collections::HashMap;

use anyhow::Result;
use fluvio_protocol::{Decoder, Encoder, Version};
use futures_util::{Stream, StreamExt};
use tracing::{info, debug};

use crate::KVStorage;

const ENTRIES_SERIALIZATION_VERSION: Version = 0;

#[allow(async_fn_in_trait)]
pub trait Log<E = Vec<u8>> {
    async fn read_from_end(&self) -> Result<impl Stream<Item = Result<E>>>;

    async fn append(&mut self, entry: E) -> Result<()> {
        self.append_batch(vec![entry]).await
    }

    async fn append_batch(&mut self, entries: Vec<E>) -> Result<()>;
}

#[derive(Debug)]
pub struct LogBasedKVStorage<K, V, L> {
    cache: HashMap<K, V>,
    log: EntriesLog<L>,
}

impl<K, V, L> KVStorage<K, V> for LogBasedKVStorage<K, V, L>
where
    K: Encoder + Decoder + Hash + Eq + Clone,
    V: Encoder + Decoder + Clone,
    L: Log,
{
    async fn get(&self, key: &K) -> Result<Option<V>> {
        Ok(self.cache.get(key).cloned())
    }

    async fn delete(&mut self, key: &K) -> Result<()> {
        self.cache.remove(key);
        self.log
            .append(Entry::RecordDeleted { key: encode(key)? })
            .await
    }

    async fn put(&mut self, key: impl Into<K>, value: impl Into<V>) -> Result<()> {
        let key = key.into();
        let value = value.into();
        self.log
            .append(Entry::RecordUpdated {
                key: encode(&key)?,
                value: encode(&value)?,
            })
            .await?;
        self.cache.insert(key, value);
        Ok(())
    }

    async fn entries(&self) -> Result<Vec<(K, V)>> {
        Ok(self
            .cache
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}

impl<K, V, L> LogBasedKVStorage<K, V, L>
where
    K: Encoder + Decoder + Hash + Eq,
    V: Encoder + Decoder,
    L: Log,
{
    pub fn new(log: L) -> Self {
        let log: EntriesLog<L> = EntriesLog::new(log);
        let cache = Default::default();
        Self { cache, log }
    }

    pub async fn sync_from_log(&mut self) -> Result<()> {
        self.replay().await
    }

    pub async fn flush(&mut self) -> Result<()> {
        let mut records = Vec::with_capacity(self.cache.len() + 1);
        records.push(Entry::Checkpoint);
        for (key, value) in self.cache.iter() {
            records.push(Entry::RecordUpdated {
                key: encode(key)?,
                value: encode(value)?,
            });
        }
        self.log.append_batch(records).await
    }

    async fn replay(&mut self) -> Result<()> {
        let mut new_cache = HashMap::new();

        let last_entries = self
            .log
            .read_from_end()
            .await?
            .take_while(|entry| match entry {
                Ok(Entry::Checkpoint) => futures_util::future::ready(false),
                _ => futures_util::future::ready(true),
            })
            .collect::<Vec<Result<Entry>>>()
            .await;

        for entry in last_entries.into_iter().rev() {
            match entry? {
                Entry::RecordUpdated { key, value } => {
                    new_cache.insert(decode(key)?, decode(value)?);
                }
                Entry::RecordDeleted { key } => {
                    new_cache.remove(&decode(key)?);
                }
                Entry::Checkpoint => anyhow::bail!("unexpected checkpoint entry"),
            };
        }

        self.cache = new_cache;
        info!(size = self.cache.len(), "read into cache");
        Ok(())
    }
}

#[derive(Debug, Default, Encoder, Decoder)]
enum Entry {
    #[fluvio(tag = 0)]
    RecordUpdated { key: Vec<u8>, value: Vec<u8> },
    #[fluvio(tag = 1)]
    RecordDeleted { key: Vec<u8> },
    #[default]
    #[fluvio(tag = 2)]
    Checkpoint,
}

#[derive(Debug)]
struct EntriesLog<L> {
    inner_log: L,
}

impl<L: Log> EntriesLog<L> {
    pub fn new(inner_log: L) -> Self {
        Self { inner_log }
    }
}

impl<L: Log> Log<Entry> for EntriesLog<L> {
    async fn read_from_end(&self) -> Result<impl Stream<Item = Result<Entry>>> {
        Ok(self.inner_log.read_from_end().await?.map(|e| {
            let entry = e?;
            debug!(len = entry.len(), "decoding entry");
            decode(entry)
        }))
    }

    async fn append_batch(&mut self, entries: Vec<Entry>) -> Result<()> {
        debug!(count = entries.len(), "encoding entries");
        let entries = entries.iter().map(encode).collect::<Result<Vec<_>, _>>()?;
        self.inner_log.append_batch(entries).await
    }
}

impl Log for &mut Vec<Vec<u8>> {
    async fn read_from_end(&self) -> Result<impl Stream<Item = Result<Vec<u8>>>> {
        Ok(futures_util::stream::iter(
            self.iter().rev().map(|e| Ok(e.clone())),
        ))
    }

    async fn append_batch(&mut self, mut entries: Vec<Vec<u8>>) -> Result<()> {
        Vec::append(self, &mut entries);
        Ok(())
    }
}

fn encode<T: Encoder>(item: &T) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    item.encode(&mut encoded, ENTRIES_SERIALIZATION_VERSION)?;
    Ok(encoded)
}

fn decode<T: Decoder>(bytes: Vec<u8>) -> Result<T> {
    let mut instance = T::default();
    instance.decode(&mut Cursor::new(bytes), ENTRIES_SERIALIZATION_VERSION)?;
    Ok(instance)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[fluvio_future::test]
    async fn test_record_inserted_and_deleted() {
        //given
        let mut log: Vec<Vec<u8>> = Vec::new();
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        let key = "key".to_string();
        //when
        storage.put(&key, "value").await.expect("inserted");

        //then
        assert_eq!(
            storage.get(&key).await.expect("read"),
            Some("value".to_string())
        );
        storage.delete(&key).await.expect("deleted");
        assert_eq!(storage.get(&key).await.expect("read"), None);
    }

    #[fluvio_future::test]
    async fn test_record_inserted_and_replayed_without_checkpoints() {
        //given
        let mut log: Vec<Vec<u8>> = Vec::new();
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        let key = "key".to_string();

        //when
        storage.put(&key, "value").await.expect("inserted");
        drop(storage);
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        assert_eq!(storage.get(&key).await.expect("read"), None);
        storage.sync_from_log().await.expect("synced");

        //then
        assert_eq!(
            storage.get(&key).await.expect("read"),
            Some("value".to_string())
        );
    }

    #[fluvio_future::test]
    async fn test_record_inserted_and_replayed_with_checkpoints() {
        //given
        let mut log: Vec<Vec<u8>> = Vec::new();
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        let key = "key".to_string();

        //when
        storage.put(&key, "value").await.expect("inserted");
        storage.flush().await.expect("flushed");
        drop(storage);

        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        assert_eq!(storage.get(&key).await.expect("read"), None);
        storage.sync_from_log().await.expect("synced");

        //then
        assert_eq!(
            storage.get(&key).await.expect("read"),
            Some("value".to_string())
        );
    }

    #[fluvio_future::test]
    async fn test_record_deleted() {
        //given
        let mut log: Vec<Vec<u8>> = Vec::new();
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        let key = "key".to_string();

        //when
        storage.put(&key, "value").await.expect("inserted");
        assert_eq!(
            storage.get(&key).await.expect("read"),
            Some("value".to_string())
        );
        storage.delete(&key).await.expect("deleted");
        drop(storage);
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        storage.sync_from_log().await.expect("synced");

        //then
        assert_eq!(storage.get(&key).await.expect("read"), None);
    }

    #[fluvio_future::test]
    async fn test_record_deleted_with_checkpoints() {
        //given
        let mut log: Vec<Vec<u8>> = Vec::new();
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        let key = "key".to_string();

        //when
        storage.put(&key, "value1").await.expect("inserted");
        storage.flush().await.expect("flushed");
        storage.delete(&key).await.expect("deleted");
        storage.flush().await.expect("flushed");
        storage.put(&key, "value2").await.expect("inserted");
        storage.flush().await.expect("flushed");
        storage.delete(&key).await.expect("deleted");
        storage.flush().await.expect("flushed");
        drop(storage);
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        storage.sync_from_log().await.expect("synced");

        //then
        assert_eq!(storage.get(&key).await.expect("read"), None);
    }

    #[fluvio_future::test]
    async fn test_record_inserted_many_versions() {
        //given
        let mut log: Vec<Vec<u8>> = Vec::new();
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        let key = "key".to_string();

        //when
        storage.put(&key, "value1").await.expect("inserted");
        storage.put(&key, "value2").await.expect("inserted");
        storage.flush().await.expect("flushed");
        storage.put(&key, "value3").await.expect("inserted");
        drop(storage);
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        storage.sync_from_log().await.expect("synced");

        //then
        assert_eq!(
            storage.get(&key).await.expect("read"),
            Some("value3".to_string())
        );
    }

    #[fluvio_future::test]
    async fn test_sync_read_until_first_checkpoint() {
        //given
        let mut log: Vec<Vec<u8>> = Vec::new();
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();

        //when
        storage.put(&key1, "value1").await.expect("inserted");
        storage.flush().await.expect("flushed");
        drop(storage);
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        storage.put(&key2, "value2").await.expect("inserted");
        storage.flush().await.expect("flushed");
        drop(storage);

        //then
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        storage.sync_from_log().await.expect("synced");

        assert_eq!(storage.get(&key1).await.expect("read"), None);
        assert_eq!(
            storage.get(&key2).await.expect("read"),
            Some("value2".to_string())
        );
    }

    #[fluvio_future::test]
    async fn test_record_decoding_error() {
        //given
        let mut log: Vec<Vec<u8>> = vec![b"wrong input".to_vec()];
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);

        //when
        let sync_res = storage.sync_from_log().await;

        //then
        assert!(sync_res.is_err());
        assert!(sync_res
            .unwrap_err()
            .to_string()
            .contains("Unknown Entry type"));
    }

    #[fluvio_future::test]
    async fn test_list_all_entries() {
        //given
        let mut log: Vec<Vec<u8>> = Vec::new();
        let mut storage = LogBasedKVStorage::<String, String, _>::new(&mut log);
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        let key3 = "key3".to_string();
        //when
        storage.put(&key1, "value").await.expect("inserted");
        storage.put(&key1, "another_value").await.expect("updated");
        storage.put(&key2, "value2").await.expect("inserted 2");
        storage.put(&key3, "value3").await.expect("inserted 3");
        storage.delete(&key3).await.expect("deleted");

        //then
        let mut entries = storage.entries().await.expect("entries");
        entries.sort();
        assert_eq!(
            entries,
            vec![
                (key1, "another_value".to_string()),
                (key2, "value2".to_string())
            ]
        );
    }
}
