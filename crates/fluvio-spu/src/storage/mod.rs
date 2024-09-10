use std::sync::Arc;
use std::fmt::Debug;
use std::time::Instant;

use tracing::{debug, instrument};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use anyhow::Result;

use fluvio_protocol::record::BatchRecords;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_spu_schema::Isolation;
use fluvio_protocol::Encoder;
use fluvio_protocol::record::{Offset, RecordSet};
use fluvio_protocol::link::ErrorCode;
use fluvio_storage::{ReplicaStorage, StorageError, OffsetInfo, ReplicaSlice};
use fluvio_types::event::offsets::OffsetChangeListener;
use fluvio_types::event::offsets::OffsetPublisher;

pub const REMOVAL_START: Offset = -1000; // indicate that storage about to be removed
pub const REMOVAL_END: Offset = -1001; // indicate the storage has been removed

/// Thread safe storage for replicas
#[derive(Debug)]
pub struct SharableReplicaStorage<S> {
    id: ReplicaKey,
    inner: Arc<RwLock<S>>,
    leo: Arc<OffsetPublisher>,
    hw: Arc<OffsetPublisher>,
}

impl<S> Clone for SharableReplicaStorage<S> {
    fn clone(&self) -> Self {
        Self {
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
    pub async fn create(id: ReplicaKey, config: S::ReplicaConfig) -> Result<Self> {
        let storage = S::create_or_load(&id, config).await?;

        let leo = Arc::new(OffsetPublisher::new(storage.get_leo()));
        let hw = Arc::new(OffsetPublisher::new(storage.get_hw()));
        Ok(Self {
            id,
            inner: Arc::new(RwLock::new(storage)),
            leo,
            hw,
        })
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

    pub fn as_offset(&self) -> OffsetInfo {
        OffsetInfo {
            hw: self.hw(),
            leo: self.leo(),
        }
    }

    /// listen to offset based on isolation
    pub fn offset_listener(&self, isolation: &Isolation) -> OffsetChangeListener {
        match isolation {
            Isolation::ReadCommitted => self.hw.change_listener(),
            Isolation::ReadUncommitted => self.leo.change_listener(),
        }
    }

    /// readable ref to storage
    pub async fn read(&self) -> RwLockReadGuard<'_, S> {
        self.inner.read().await
    }

    /// writable ref to storage
    pub async fn write(&self) -> RwLockWriteGuard<'_, S> {
        self.inner.write().await
    }

    /// get start offset and hw
    pub async fn start_offset_info(&self) -> (Offset, Offset) {
        let reader = self.read().await;
        (reader.get_log_start_offset(), reader.get_hw())
    }

    /// read records into partition response
    /// return leo and hw
    #[instrument(skip(self, offset, max_len, isolation))]
    pub async fn read_records(
        &self,
        offset: Offset,
        max_len: u32,
        isolation: Isolation,
    ) -> Result<ReplicaSlice, ErrorCode> {
        let read_storage = self.read().await;

        read_storage
            .read_partition_slice(offset, max_len, isolation)
            .await
    }

    pub async fn update_hw(&self, hw: Offset) -> Result<bool, StorageError> {
        let mut writer = self.write().await;
        if writer.update_high_watermark(hw).await? {
            self.hw.update(hw);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[instrument(skip(self, records, hw_update))]
    pub async fn write_record_set<R: BatchRecords>(
        &self,
        records: &mut RecordSet<R>,
        hw_update: bool,
    ) -> Result<(Offset, Offset, usize)> {
        debug!(
            replica = %self.id,
            leo = self.leo(),
            hw = self.hw(),
            hw_update,
            records = records.total_records(),
            size = records.write_size(0)
        );

        let mut writer = self.write().await;

        let base_offset = writer.get_leo();

        let now = Instant::now();
        let bytes_written = writer.write_recordset(records, hw_update).await?;
        debug!(write_time_ms = %now.elapsed().as_millis());

        let leo = writer.get_leo();
        debug!(leo, "updated leo");
        self.leo.update(leo);
        if hw_update {
            let hw = writer.get_hw();
            debug!(hw, "updated hw");
            self.hw.update(hw);
        }

        Ok((base_offset, leo, bytes_written))
    }

    /// perform permanent remove
    pub async fn remove(&self) -> Result<(), StorageError> {
        self.leo.update(REMOVAL_START);
        let writer = self.write().await;
        writer.remove().await?;
        self.leo.update(REMOVAL_END);
        Ok(())
    }
}
