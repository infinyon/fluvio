use std::{
    time::SystemTime,
    sync::Arc,
    collections::{HashMap, hash_map::Entry},
    ops::AddAssign,
};

use anyhow::Result;
use async_lock::RwLock;
use tracing::trace;

use fluvio_kv_storage::KVStorage;
use fluvio_protocol::{record::ReplicaKey, Encoder, Decoder};
use fluvio_storage::FileReplica;

use crate::replication::leader::{
    LeaderKVStorage, FollowerNotifier, LeaderReplicaState, LeaderReplicaLog,
};

pub(crate) type TimestampSecs = u64;

const DEFAULT_FLUSH_THRESHOLD: usize = 100;

#[derive(Debug, Default)]
pub(crate) struct SharedConsumerOffsetStorages(
    Arc<RwLock<HashMap<ReplicaKey, SharableConsumerOffsetStorage>>>,
);

#[derive(Debug, Clone)]
pub(crate) struct SharableConsumerOffsetStorage(Arc<RwLock<ConsumerOffsetStorage>>);

#[derive(Debug, Hash, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Encoder, Decoder)]
pub(crate) struct ConsumerOffsetKey {
    pub replica_id: ReplicaKey,
    pub consumer_id: String,
}
/// Consumer offset value. Keeps the last offset seen by a consumer, and
/// the modification time (UTC timestamp in seconds).
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Encoder, Decoder)]
pub(crate) struct ConsumerOffset {
    pub offset: i64,
    pub modified_time: TimestampSecs,
}

#[derive(Debug)]
pub(crate) struct ConsumerOffsetStorage {
    kv: LeaderKVStorage<ConsumerOffsetKey, ConsumerOffset, FileReplica>,
    flush_threshold: usize,
    changes_since_flush: usize,
}

impl SharedConsumerOffsetStorages {
    pub(crate) async fn get_or_insert(
        &self,
        replica: &LeaderReplicaState<FileReplica>,
        notifier: &Arc<FollowerNotifier>,
    ) -> Result<SharableConsumerOffsetStorage> {
        let mut write = self.0.write().await;
        match write.entry(replica.id().clone()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let mut storage = ConsumerOffsetStorage::new(replica.clone(), notifier.clone());
                storage.kv.sync_from_log().await?;
                let shared: SharableConsumerOffsetStorage = storage.into();
                entry.insert(shared.clone());
                Ok(shared)
            }
        }
    }
}

impl ConsumerOffsetStorage {
    pub fn new(
        replica: LeaderReplicaState<FileReplica>,
        follower_notifier: Arc<FollowerNotifier>,
    ) -> Self {
        Self::with(replica, follower_notifier, DEFAULT_FLUSH_THRESHOLD)
    }

    pub fn with(
        replica: LeaderReplicaState<FileReplica>,
        follower_notifier: Arc<FollowerNotifier>,
        flush_threshold: usize,
    ) -> Self {
        Self {
            kv: LeaderKVStorage::new(LeaderReplicaLog::new(replica, follower_notifier)),
            flush_threshold,
            changes_since_flush: Default::default(),
        }
    }

    async fn maybe_flush(&mut self) -> Result<()> {
        if self.changes_since_flush > self.flush_threshold {
            self.kv.flush().await?;
            self.changes_since_flush = Default::default();
        }
        Ok(())
    }
}

impl KVStorage<ConsumerOffsetKey, ConsumerOffset> for ConsumerOffsetStorage {
    async fn get(&self, key: &ConsumerOffsetKey) -> Result<Option<ConsumerOffset>> {
        trace!(?key, "get");
        self.kv.get(key).await
    }

    async fn delete(&mut self, key: &ConsumerOffsetKey) -> Result<()> {
        trace!(?key, "delete");
        let result = self.kv.delete(key).await;
        self.changes_since_flush.add_assign(1);
        self.maybe_flush().await?;
        result
    }

    async fn put(
        &mut self,
        key: impl Into<ConsumerOffsetKey>,
        value: impl Into<ConsumerOffset>,
    ) -> Result<()> {
        let key = key.into();
        let value = value.into();
        trace!(?key, ?value, "put");
        let result = self.kv.put(key, value).await;
        self.changes_since_flush.add_assign(1);
        self.maybe_flush().await?;
        result
    }

    async fn entries(&self) -> Result<Vec<(ConsumerOffsetKey, ConsumerOffset)>> {
        trace!("entries");
        self.kv.entries().await
    }
}

impl ConsumerOffsetKey {
    pub(crate) fn new(replica_id: impl Into<ReplicaKey>, consumer_id: impl Into<String>) -> Self {
        Self {
            replica_id: replica_id.into(),
            consumer_id: consumer_id.into(),
        }
    }
}
impl ConsumerOffset {
    pub(crate) fn new(offset: i64) -> Self {
        let now = now_timestamp();
        Self::with(offset, now)
    }

    pub(crate) fn with(offset: i64, modified_time: TimestampSecs) -> Self {
        Self {
            offset,
            modified_time,
        }
    }
}

impl From<ConsumerOffsetStorage> for SharableConsumerOffsetStorage {
    fn from(value: ConsumerOffsetStorage) -> Self {
        Self(Arc::new(RwLock::new(value)))
    }
}

impl SharableConsumerOffsetStorage {
    pub async fn get(&self, key: &ConsumerOffsetKey) -> Result<Option<ConsumerOffset>> {
        self.0.read().await.get(key).await
    }

    pub async fn delete(&self, key: &ConsumerOffsetKey) -> Result<()> {
        self.0.write().await.delete(key).await
    }

    pub async fn put(
        &self,
        key: impl Into<ConsumerOffsetKey>,
        value: impl Into<ConsumerOffset>,
    ) -> Result<()> {
        self.0.write().await.put(key, value).await
    }

    pub async fn list(&self) -> Result<Vec<(ConsumerOffsetKey, ConsumerOffset)>> {
        self.0.read().await.entries().await
    }
}

fn now_timestamp() -> TimestampSecs {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::{env::temp_dir, path::Path};

    use fluvio_controlplane::replica::Replica;
    use fluvio_storage::config::ReplicaConfig;
    use fluvio_types::PartitionId;
    use flv_util::fixture::ensure_clean_dir;

    use crate::{
        config::ReplicationConfig, storage::SharableReplicaStorage,
        control_plane::StatusLrsMessageSink,
    };

    use super::*;

    #[fluvio_future::test]
    async fn test_flush_invoked_no_records() {
        //given
        let leader = create_offset_replica("test_flush_invoked").await;
        let notifier = FollowerNotifier::shared();
        let mut storage = ConsumerOffsetStorage::with(leader.clone(), notifier, 1);
        let key1 = ConsumerOffsetKey::new(("topic1", 0), "consumer1");

        //when
        assert_eq!(leader.hw(), 0);
        storage.delete(&key1).await.expect("put record1");
        assert_eq!(leader.hw(), 1);
        storage.delete(&key1).await.expect("put record1");
        assert_eq!(leader.hw(), 3);

        leader.remove().await.expect("removed");
        //then
    }

    #[fluvio_future::test]
    async fn test_flush_invoked_with_records() {
        //given
        let leader = create_offset_replica("test_flush_invoked_with_records").await;
        let notifier = FollowerNotifier::shared();
        let mut storage = ConsumerOffsetStorage::with(leader.clone(), notifier, 1);
        let key1 = ConsumerOffsetKey::new(("topic1", 0), "consumer1");
        let value1 = ConsumerOffset::with(1, now_timestamp() - 100);

        //when
        assert_eq!(leader.hw(), 0);
        storage
            .put(key1.clone(), value1.clone())
            .await
            .expect("put record1");
        assert_eq!(leader.hw(), 1);
        storage.put(key1, value1).await.expect("put record1");
        assert_eq!(leader.hw(), 4);

        leader.remove().await.expect("removed");
        //then
    }

    async fn create_offset_replica(dir: impl AsRef<Path>) -> LeaderReplicaState<FileReplica> {
        let base_dir = temp_dir().join(dir);
        ensure_clean_dir(&base_dir);
        let config = ReplicaConfig {
            base_dir,
            ..Default::default()
        };
        let replica_id = ReplicaKey::new("topic", PartitionId::default());
        let replication_config = ReplicationConfig::default();
        let replica = Replica::new(replica_id.clone(), 5000, vec![5000]);
        let status_update = StatusLrsMessageSink::shared();

        let storage = SharableReplicaStorage::create(replica_id, config)
            .await
            .expect("storage");
        LeaderReplicaState::new(replica, replication_config, status_update, storage).into_inner()
    }
}
