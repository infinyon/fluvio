use std::sync::Arc;
use std::fmt::Debug;
use std::time::Instant;

use tracing::{debug};
use async_rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use fluvio_controlplane_metadata::partition::{ReplicaKey};
use dataplane::{Isolation, record::RecordSet};
use dataplane::core::Encoder;
use dataplane::{Offset};
use fluvio_storage::{ReplicaStorage, SlicePartitionResponse, StorageError};
use fluvio_types::{SpuId, event::offsets::OffsetChangeListener};
use fluvio_types::event::offsets::OffsetPublisher;

/// Thread safe storage for replicas
#[derive(Debug)]
pub struct SharableReplicaStorage<S> {
    leader: SpuId,
    id: ReplicaKey,
    inner: Arc<RwLock<S>>,
    leo: Arc<OffsetPublisher>,
    hw: Arc<OffsetPublisher>,
}

impl<S> Clone for SharableReplicaStorage<S> {
    fn clone(&self) -> Self {
        Self {
            leader: self.leader,
            id: self.id.clone(),
            inner: self.inner.clone(),
            leo: self.leo.clone(),
            hw: self.hw.clone(),
        }
    }
}

impl<S> SharableReplicaStorage<S>
where
    S: ReplicaStorage,
{
    /// create new storage replica or restore from durable storage based on configuration
    pub async fn create(
        leader: SpuId,
        id: ReplicaKey,
        config: S::Config,
    ) -> Result<Self, StorageError> {
        let storage = S::create(&id, leader, config).await?;

        let leo = Arc::new(OffsetPublisher::new(storage.get_leo()));
        let hw = Arc::new(OffsetPublisher::new(storage.get_hw()));
        Ok(Self {
            leader,
            id,
            inner: Arc::new(RwLock::new(storage)),
            leo,
            hw,
        })
    }

    pub fn leader(&self) -> &SpuId {
        &self.leader
    }

    pub fn set_leader(&mut self, spu: SpuId) {
        self.leader = spu;
    }

    pub fn id(&self) -> &ReplicaKey {
        &self.id
    }

    /// log end offset
    pub fn leo(&self) -> Offset {
        self.leo.current_value()
    }

    /// high watermark
    pub fn hw(&self) -> Offset {
        self.hw.current_value()
    }

    /// listen to offset based on isolation
    pub fn offset_listener(&self, isolation: &Isolation) -> OffsetChangeListener {
        match isolation {
            Isolation::ReadCommitted => self.hw.change_listner(),
            Isolation::ReadUncommitted => self.leo.change_listner(),
        }
    }

    /// readable ref to storage
    async fn read(&self) -> RwLockReadGuard<'_, S> {
        self.inner.read().await
    }

    /// writable ref to storage
    async fn write(&self) -> RwLockWriteGuard<'_, S> {
        self.inner.write().await
    }

    /// get start offset and hw
    pub async fn start_offset_info(&self) -> (Offset, Offset) {
        let reader = self.read().await;
        (reader.get_log_start_offset(), reader.get_hw())
    }

    /// read records into partition response
    /// return hw and leo
    pub async fn read_records<P>(
        &self,
        offset: Offset,
        max_len: u32,
        isolation: Isolation,
        partition_response: &mut P,
    ) -> (Offset, Offset)
    where
        P: SlicePartitionResponse + Send,
    {
        let read_storage = self.read().await;

        read_storage
            .read_partition_slice(offset, max_len, isolation, partition_response)
            .await
    }

    pub async fn update_hw(&self, hw: Offset) -> Result<bool, StorageError> {
        let mut writer = self.write().await;
        writer.update_high_watermark(hw).await
    }

    pub async fn write_record_set(
        &self,
        records: &mut RecordSet,
        hw_update: bool,
    ) -> Result<(), StorageError> {
        debug!(
            replica = %self.id,
            leo = self.leo(),
            hw = self.hw(),
            hw_update,
            records = records.total_records(),
            size = records.write_size(0)
        );

        let mut writer = self.write().await;
        let now = Instant::now();
        let _offset_updates = writer.write_recordset(records, hw_update).await?;
        debug!(write_time_ms = %now.elapsed().as_millis());

        let leo = writer.get_leo();
        debug!(leo, "updated leo");
        self.leo.update(leo);
        if hw_update {
            let hw = writer.get_hw();
            debug!(hw, "updated hw");
            self.hw.update(hw);
        }

        Ok(())
    }

    /// perform permanent remove
    pub async fn remove(&self) -> Result<(), StorageError> {
        let writer = self.write().await;
        writer.remove().await
    }
}
