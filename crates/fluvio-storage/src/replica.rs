use std::cmp::min;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{debug, trace, warn, instrument, info};
use async_trait::async_trait;
use anyhow::Result;

use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_protocol::Encoder;
use fluvio_future::fs::{create_dir_all, remove_dir_all};
use fluvio_protocol::link::ErrorCode;
use fluvio_spu_schema::Isolation;
use fluvio_protocol::record::{Offset, ReplicaKey, Size, Size64};
use fluvio_protocol::record::{Batch, BatchRecords};
use fluvio_protocol::record::RecordSet;

use crate::{OffsetInfo, checkpoint::CheckPoint};
use crate::segments::SharedSegments;
use crate::segment::MutableSegment;
use crate::config::{ReplicaConfig, SharedReplicaConfig, StorageConfig};
use crate::ReplicaSlice;
use crate::{StorageError, ReplicaStorage};
use crate::cleaner::Cleaner;

/// Replica is public abstraction for commit log which are distributed.
/// Internally it is stored as list of segments.  Each segment contains finite sets of record batches.
///
#[derive(Debug)]
pub struct FileReplica {
    #[allow(dead_code)]
    last_base_offset: Offset,
    #[allow(dead_code)]
    partition: Size,
    option: Arc<SharedReplicaConfig>,
    active_segment: MutableSegment,
    prev_segments: Arc<SharedSegments>,
    commit_checkpoint: CheckPoint<Offset>,
    cleaner: Arc<Cleaner>,
    size: Arc<ReplicaSize>,
}

#[derive(Debug, Default)]
pub(crate) struct ReplicaSize {
    active_segment: AtomicU64,
    prev_segments: AtomicU64,
}

impl Unpin for FileReplica {}

#[async_trait]
impl ReplicaStorage for FileReplica {
    type ReplicaConfig = ReplicaConfig;

    async fn create_or_load(
        replica: &ReplicaKey,
        replica_config: Self::ReplicaConfig,
    ) -> Result<Self> {
        let storage_config = StorageConfig::builder()
            .build()
            .map_err(|err| StorageError::Other(format!("failed to build cleaner config: {err}")))?;

        Self::create_or_load_with_storage(
            replica.topic.clone(),
            replica.partition,
            0,
            replica_config,
            Arc::new(storage_config),
        )
        .await
    }

    #[inline(always)]
    fn get_hw(&self) -> Offset {
        *self.commit_checkpoint.get_offset()
    }

    /// offset mark that beginning of uncommitted
    #[inline(always)]
    fn get_leo(&self) -> Offset {
        self.active_segment.get_end_offset()
    }

    /// earliest offset
    fn get_log_start_offset(&self) -> Offset {
        let min_base_offset = self.prev_segments.min_offset();
        if min_base_offset < 0 {
            self.active_segment.get_base_offset()
        } else {
            min_base_offset
        }
    }

    /// read partition slice
    /// return leo, hw
    #[instrument(skip(self, offset, max_len, isolation))]
    async fn read_partition_slice(
        &self,
        offset: Offset,
        max_len: u32,
        isolation: Isolation,
    ) -> Result<ReplicaSlice, ErrorCode> {
        match isolation {
            Isolation::ReadCommitted => {
                self.read_records(offset, Some(self.get_hw()), max_len)
                    .await
            }
            Isolation::ReadUncommitted => self.read_records(offset, None, max_len).await,
        }
    }

    /// return the size in bytes (includes index size and log size)
    #[instrument(skip(self))]
    fn get_partition_size(&self) -> Size64 {
        let active_len = self.size.get_active();
        debug!(active_len, "Active segment length");
        let total_prev_segments_len = self.size.get_prev();
        debug!(
            total_prev_segments_len,
            "Cumulated previous segments length"
        );
        total_prev_segments_len + active_len
    }

    /// write records to this replica
    /// if update_highwatermark is set, set high watermark is end
    //  this is used when LRS = 1
    #[instrument(skip(self, records, update_highwatermark))]
    async fn write_recordset<R: BatchRecords>(
        &mut self,
        records: &mut RecordSet<R>,
        update_highwatermark: bool,
    ) -> Result<usize> {
        let max_batch_size = self.option.max_batch_size.get() as usize;
        let mut total_size = 0;
        // check if any of the records's batch exceed max length
        for batch in &records.batches {
            let batch_size = batch.write_size(0);
            total_size += batch_size;
            if batch_size > max_batch_size {
                return Err(StorageError::BatchTooBig(max_batch_size).into());
            }
        }

        for batch in &mut records.batches {
            self.write_batch(batch).await?;
        }

        if update_highwatermark {
            self.update_high_watermark_to_end().await?;
        }
        Ok(total_size)
    }

    /// update committed offset (high watermark)
    /// if true, hw is updated
    #[instrument(skip(self, offset))]
    async fn update_high_watermark(&mut self, offset: Offset) -> Result<bool, StorageError> {
        let old_offset = self.get_hw();
        if old_offset == offset {
            trace!(
                "new high watermark: {} is same as existing one, skipping",
                offset
            );
            Ok(false)
        } else {
            trace!(
                "updating to new high watermark: {} old: {}",
                old_offset,
                offset
            );
            self.commit_checkpoint.write(offset).await?;
            Ok(true)
        }
    }

    #[instrument(skip(self))]
    async fn remove(&self) -> Result<(), StorageError> {
        remove_dir_all(&self.option.base_dir)
            .await
            .map_err(StorageError::Io)?;

        self.cleaner.shutdown();
        Ok(())
    }
}

impl FileReplica {
    pub const PREFER_MAX_LEN: u32 = 1000000; // 1MB as limit

    /// Construct a new replica with specified topic and partition.
    /// It can start with arbitrary offset.  However, for normal replica,
    /// it is usually starts with 0.  
    ///
    /// Replica is minimum unit of logs that will can be replicated.  
    /// It is a unique pair of (topic,partition)
    ///
    /// Replica will use base directory to create it's own directory.
    /// Directory name will encode unique replica id which is combination of topic
    /// and partition.
    ///
    /// If there is existing directory then it will load existing logs.
    /// The logs will be validated to ensure it's safe to use it.
    /// It is possible logs can't be used because they may be corrupted.
    #[instrument(skip(topic, partition, base_offset, replica_config, storage_config))]
    pub async fn create_or_load_with_storage<S>(
        topic: S,
        partition: Size,
        base_offset: Offset,
        replica_config: ReplicaConfig,
        storage_config: Arc<StorageConfig>,
    ) -> Result<FileReplica>
    where
        S: AsRef<str> + Send + 'static,
    {
        let replica_dir = replica_config
            .base_dir
            .join(replica_dir_name(topic, partition));

        info!("creating rep dir: {}", replica_dir.display());
        debug!("replica config: {:?}", replica_config);
        create_dir_all(&replica_dir).await?; // ensure dir_name exits

        let mut rep_option = replica_config.clone();
        rep_option.base_dir = replica_dir;

        let shared_config: Arc<SharedReplicaConfig> = Arc::new(rep_option.into());

        let (segments, last_offset_res) = SharedSegments::from_dir(shared_config.clone()).await?;

        let active_segment = if let Some(last_offset) = last_offset_res {
            debug!(last_offset, "last segment found, validating offsets");
            let mut last_segment =
                MutableSegment::open_for_write(last_offset, shared_config.clone()).await?;
            last_segment.validate_and_repair().await?;
            info!(
                end_offset = last_segment.get_end_offset(),
                "existing segment validated with last offset",
            );
            last_segment
        } else {
            info!("no existing segment found, creating new one");
            MutableSegment::create(base_offset, shared_config.clone()).await?
        };

        let last_base_offset = active_segment.get_base_offset();

        let mut commit_checkpoint: CheckPoint<Offset> =
            CheckPoint::create(shared_config.clone(), "replication.chk", last_base_offset).await?;

        // ensure checkpoint is valid
        let hw = *commit_checkpoint.get_offset();
        let leo = active_segment.get_end_offset();
        if hw > leo {
            info!(
                hw,
                leo, "high watermark is greater than log end offset, resetting to leo"
            );
            commit_checkpoint.write(leo).await?;
        }

        let size = Arc::new(ReplicaSize::default());
        let cleaner = Cleaner::start_new(
            storage_config,
            shared_config.clone(),
            segments.clone(),
            size.clone(),
        );

        Ok(Self {
            option: shared_config,
            last_base_offset,
            partition,
            active_segment,
            prev_segments: segments,
            commit_checkpoint,
            cleaner,
            size,
        })
    }

    /// clear the any holding directory for replica
    #[instrument(skip(replica, option))]
    pub async fn clear(replica: &ReplicaKey, option: &SharedReplicaConfig) {
        let replica_dir = option
            .base_dir
            .join(replica_dir_name(&replica.topic, replica.partition));

        info!("removing dir: {}", replica_dir.display());
        if let Err(err) = remove_dir_all(&replica_dir).await {
            warn!("error trying to remove: {:#?}, err: {}", replica_dir, err);
        }
    }

    /// update high watermark to end
    #[instrument(skip(self))]
    pub async fn update_high_watermark_to_end(&mut self) -> Result<bool, StorageError> {
        self.update_high_watermark(self.get_leo()).await
    }

    /// read all uncommitted records
    #[allow(unused)]
    #[instrument(skip(self, max_len))]
    pub async fn read_all_uncommitted_records(
        &self,
        max_len: u32,
    ) -> Result<ReplicaSlice, ErrorCode> {
        self.read_records(self.get_hw(), None, max_len).await
    }

    /// read record slice into response
    /// * `start_offset`:  start offsets
    /// * `max_offset`:  max offset (exclusive)
    /// * `responsive`:  output
    /// * `max_len`:  max length of the slice
    //  return leo, hw
    #[instrument(skip(self))]
    async fn read_records(
        &self,
        start_offset: Offset,
        max_offset: Option<Offset>,
        max_len: u32,
    ) -> Result<ReplicaSlice, ErrorCode> {
        let hw = self.get_hw();
        let leo = self.get_leo();
        debug!(hw, leo, "starting read records",);

        let mut slice = ReplicaSlice {
            end: OffsetInfo { hw, leo },
            start: self.get_log_start_offset(),
            ..Default::default()
        };

        let active_base_offset = self.active_segment.get_base_offset();
        let file_slice = if start_offset >= active_base_offset {
            debug!(start_offset, active_base_offset, "is in active segment");
            if start_offset == leo {
                trace!("start offset is same as end offset, skipping");
                return Ok(slice);
            } else if start_offset > leo {
                return Err(ErrorCode::Other(format!(
                    "start offset: {start_offset} is greater than leo: {leo}"
                )));
            } else if let Some(slice) = self
                .active_segment
                .records_slice(start_offset, max_offset)
                .await?
            {
                slice
            } else {
                return Err(ErrorCode::Other(format!(
                    "no records found in active replica, start: {}, max: {:#?}, active: {:#?}",
                    start_offset, max_offset, self.active_segment
                )));
            }
        } else {
            debug!(start_offset, active_base_offset, "not in active sgments");
            self.prev_segments
                .find_slice(start_offset, max_offset)
                .await?
        };

        let limited_slice = AsyncFileSlice::new(
            file_slice.fd(),
            file_slice.position(),
            min(file_slice.len(), max_len as u64),
        );

        debug!(
            fd = limited_slice.fd(),
            pos = limited_slice.position(),
            len = limited_slice.len(),
            "retrieved slice",
        );

        slice.file_slice = Some(limited_slice);
        Ok(slice)
    }

    #[instrument(skip(self, item))]
    async fn write_batch<R: BatchRecords>(&mut self, item: &mut Batch<R>) -> Result<()> {
        if !(self.active_segment.append_batch(item).await?) {
            info!(
                partition = self.partition,
                path = %self.option.base_dir.display(),
                base_offset = self.active_segment.get_base_offset(),
                "rolling over active segment");
            self.active_segment.roll_over().await?;
            let last_offset = self.active_segment.get_end_offset();
            let new_segment = MutableSegment::create(last_offset, self.option.clone()).await?;
            let old_mut_segment = mem::replace(&mut self.active_segment, new_segment);
            let old_segment = old_mut_segment.as_segment().await?;
            self.size.add_prev(old_segment.occupied_memory());
            self.prev_segments.add_segment(old_segment).await;
            self.active_segment.append_batch(item).await?;
        }
        self.size
            .store_active(self.active_segment.occupied_memory());
        Ok(())
    }
}

impl ReplicaSize {
    pub(crate) fn get(&self) -> Size64 {
        self.get_active() + self.get_prev()
    }

    pub(crate) fn store_prev(&self, prev_segments: Size64) {
        self.prev_segments.store(prev_segments, Ordering::Release);
    }

    fn store_active(&self, active: Size64) {
        self.active_segment.store(active, Ordering::Release);
    }

    fn get_active(&self) -> Size64 {
        self.active_segment.load(Ordering::Acquire)
    }

    fn add_prev(&self, prev_segment: Size64) {
        self.prev_segments
            .fetch_add(prev_segment, Ordering::Release);
    }

    fn get_prev(&self) -> Size64 {
        self.prev_segments.load(Ordering::Acquire)
    }
}

// generate replication folder name
fn replica_dir_name<S: AsRef<str>>(topic_name: S, partition_index: Size) -> String {
    format!("{}-{}", topic_name.as_ref(), partition_index)
}

#[cfg(test)]
mod tests {

    use fluvio_future::fs::remove_dir_all;
    use fluvio_future::timer::sleep;
    use futures_lite::AsyncWriteExt;
    use tracing::debug;
    use tracing::info;
    use std::env::temp_dir;
    use std::fs;
    use std::fs::metadata;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::time::Duration;

    use fluvio_spu_schema::Isolation;
    use fluvio_protocol::record::Batch;
    use fluvio_protocol::record::Offset;
    use fluvio_protocol::{Decoder, Encoder};
    use fluvio_protocol::record::{Record, RecordSet};
    use fluvio_protocol::record::MemoryRecords;
    use fluvio_protocol::fixture::{BatchProducer, create_batch, create_batch_with_producer};
    use fluvio_protocol::fixture::read_bytes_from_file;
    use flv_util::fixture::ensure_clean_dir;

    use crate::config::{ReplicaConfig, StorageConfig};
    use crate::StorageError;
    use crate::ReplicaStorage;
    use crate::fixture::storage_config;

    use super::FileReplica;

    const TEST_SEG_NAME: &str = "00000000000000000020.log";
    const TEST_SE2_NAME: &str = "00000000000000000022.log";
    const TEST_SEG_IDX: &str = "00000000000000000020.index";
    const TEST_SEG2_IDX: &str = "00000000000000000022.index";
    const START_OFFSET: Offset = 20;

    async fn create_replica(
        topic: &'static str,
        base_offset: Offset,
        config: ReplicaConfig,
    ) -> FileReplica {
        FileReplica::create_or_load_with_storage(topic, 0, base_offset, config, storage_config())
            .await
            .expect("replica")
    }

    /// create option, ensure they are clean
    fn base_option(dir: &str) -> ReplicaConfig {
        let base_dir = temp_dir().join(dir);
        ensure_clean_dir(&base_dir);
        ReplicaConfig {
            segment_max_bytes: 10000,
            base_dir,
            index_max_interval_bytes: 1000,
            index_max_bytes: 1000,
            ..Default::default()
        }
    }

    fn rollover_option(dir: &str) -> ReplicaConfig {
        let base_dir = temp_dir().join(dir);
        ensure_clean_dir(&base_dir);
        ReplicaConfig {
            segment_max_bytes: 100,
            base_dir,
            index_max_bytes: 1000,
            index_max_interval_bytes: 0,
            ..Default::default()
        }
    }

    #[fluvio_future::test]
    async fn test_replica_simple() {
        let option = base_option("test_simple");

        let mut replica = create_replica("test", START_OFFSET, option.clone()).await;

        assert_eq!(replica.get_log_start_offset(), START_OFFSET);
        assert_eq!(replica.get_leo(), START_OFFSET);
        assert_eq!(replica.get_hw(), START_OFFSET);

        replica
            .write_batch(&mut create_batch())
            .await
            .expect("send");
        assert_eq!(replica.get_leo(), START_OFFSET + 2); // 2 batches
        assert_eq!(replica.get_hw(), START_OFFSET); // hw should not change since we have not committed them

        replica
            .update_high_watermark(10)
            .await
            .expect("high watermaerk");
        assert_eq!(replica.get_hw(), 10); // hw should set to whatever we passed

        let test_file = option.base_dir.join("test-0").join(TEST_SEG_NAME);
        debug!("using test file: {:#?}", test_file);
        let bytes = read_bytes_from_file(&test_file).expect("read");

        let batch =
            Batch::<MemoryRecords>::decode_from(&mut Cursor::new(bytes), 0).expect("decode");
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.get_base_offset(), START_OFFSET);
        assert_eq!(batch.get_header().last_offset_delta, 1);
        assert_eq!(batch.records().len(), 2);

        // there should not be any segment for offset 0 since base offset is 20
        let reader = replica.prev_segments.read().await;
        assert!(reader.find_segment(0).is_none());

        //println!("segments:")
        // segment with offset 20 should be active segment
        assert!(reader.find_segment(20).is_none());
        assert!(reader.find_segment(21).is_none());
        assert!(reader.find_segment(30).is_none()); // any h
    }

    const TEST_UNCOMMIT_DIR: &str = "test_uncommitted";

    #[fluvio_future::test]
    async fn test_uncommitted_fetch() {
        let option = base_option(TEST_UNCOMMIT_DIR);

        let mut replica = create_replica("test", 0, option).await;

        assert_eq!(replica.get_leo(), 0);
        assert_eq!(replica.get_hw(), 0);

        // reading empty replica should return empyt records

        let slice = replica
            .read_all_uncommitted_records(FileReplica::PREFER_MAX_LEN)
            .await
            .expect("read");
        assert!(slice.file_slice.is_none());

        // write batches
        let mut batch = create_batch();
        let batch_len = batch.write_size(0);
        debug!("batch len: {}", batch_len);
        replica.write_batch(&mut batch).await.expect("write");

        assert_eq!(replica.get_leo(), 2); // 2
        assert_eq!(replica.get_hw(), 0);

        // read records
        let slice = replica
            .read_all_uncommitted_records(FileReplica::PREFER_MAX_LEN)
            .await
            .expect("read");
        assert_eq!(slice.file_slice.unwrap().len() as usize, batch_len);

        replica.update_high_watermark(2).await.expect("update"); // first batch
        assert_eq!(replica.get_hw(), 2);

        let mut batch = create_batch();
        let batch_len = batch.write_size(0);
        replica.write_batch(&mut batch).await.expect("write");

        let slice = replica
            .read_all_uncommitted_records(FileReplica::PREFER_MAX_LEN)
            .await
            .expect("read");
        debug!("slice: {:#?}", slice);
        assert_eq!(slice.file_slice.unwrap().len() as usize, batch_len);

        replica
            .write_batch(&mut create_batch())
            .await
            .expect("write");

        let slice = replica
            .read_all_uncommitted_records(FileReplica::PREFER_MAX_LEN)
            .await
            .expect("read");
        assert_eq!(slice.file_slice.unwrap().len() as usize, batch_len * 2);

        let slice = replica
            .read_all_uncommitted_records(50)
            .await
            .expect("read");
        assert_eq!(slice.file_slice.unwrap().len(), 50);
    }

    const TEST_OFFSET_DIR: &str = "test_offset";

    #[fluvio_future::test]
    async fn test_replica_end_offset() {
        let option = base_option(TEST_OFFSET_DIR);

        let mut rep_sink = create_replica("test", START_OFFSET, option.clone()).await;
        rep_sink
            .write_batch(&mut create_batch())
            .await
            .expect("write");
        rep_sink
            .write_batch(&mut create_batch())
            .await
            .expect("write");
        drop(rep_sink);

        // open replica
        let replica2 = create_replica("test", 0, option).await;
        assert_eq!(replica2.get_leo(), START_OFFSET + 4); // should be 24 since we added 4 records
    }

    const TEST_REPLICA_DIR: &str = "test_replica";

    // you can show log by:  RUST_LOG=commit_log=debug cargo test roll_over

    #[fluvio_future::test]
    async fn test_rep_log_roll_over() {
        let option = rollover_option(TEST_REPLICA_DIR);

        let mut replica = FileReplica::create_or_load_with_storage(
            "test",
            1,
            START_OFFSET,
            option.clone(),
            storage_config(),
        )
        .await
        .expect("create rep");

        // first batch
        debug!(">>>> sending first batch");
        let mut batches = create_batch();
        replica.write_batch(&mut batches).await.expect("write");

        // second batch
        debug!(">>>> sending second batch. this should rollover");
        let mut batches = create_batch();
        replica.write_batch(&mut batches).await.expect("write");
        debug!("finish sending next batch");

        assert_eq!(replica.get_log_start_offset(), START_OFFSET);
        let replica_dir = &option.base_dir.join("test-1");
        let dir_contents = fs::read_dir(replica_dir).expect("read_dir");
        assert_eq!(dir_contents.count(), 5, "should be 5 files");

        let seg2_file = replica_dir.join(TEST_SE2_NAME);
        let bytes = read_bytes_from_file(seg2_file).expect("file read");

        let batch =
            Batch::<MemoryRecords>::decode_from(&mut Cursor::new(bytes), 0).expect("decode");
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records().len(), 2);
        assert_eq!(batch.get_base_offset(), 22);

        let metadata_res = metadata(replica_dir.join(TEST_SEG2_IDX));
        assert!(metadata_res.is_ok());
        let metadata2 = metadata_res.unwrap();
        assert_eq!(metadata2.len(), 1000);

        let seg1_metadata = metadata(replica_dir.join(TEST_SEG_IDX)).expect("metadata");
        assert_eq!(seg1_metadata.len(), 8);
    }

    #[fluvio_future::test]
    async fn test_replica_commit() {
        let option = base_option("test_commit");
        let mut replica = create_replica("test", 0, option.clone()).await;

        let mut records = RecordSet::default().add(create_batch());

        replica
            .write_recordset(&mut records, true)
            .await
            .expect("write");

        // record contains 2 batch
        assert_eq!(replica.get_hw(), 2);

        drop(replica);

        // restore replica
        let replica = create_replica("test", 0, option).await;
        assert_eq!(replica.get_hw(), 2);
    }

    const TEST_STORAGE_SIZE_DIR: &str = "test_storage_size";

    #[fluvio_future::test]
    async fn test_replica_storage_size() {
        let option = base_option(TEST_STORAGE_SIZE_DIR);
        let mut replica = create_replica("test", 0, option.clone()).await;

        let mut records = RecordSet::default().add(create_batch());

        replica
            .write_recordset(&mut records, true)
            .await
            .expect("write");

        assert_eq!(replica.get_hw(), 2);

        let size = replica.get_partition_size();
        assert_eq!(size, 79);

        replica
            .write_recordset(&mut records, true)
            .await
            .expect("write");

        let size = replica.get_partition_size();
        assert_eq!(size, 79 * 2);
    }

    #[fluvio_future::test]
    async fn test_replica_storage_size_delete_log() {
        let option = base_option("test_storage_size_deleted");
        let mut replica = create_replica("test", 0, option.clone()).await;

        let mut records = RecordSet::default().add(create_batch());

        replica
            .write_recordset(&mut records, true)
            .await
            .expect("write");

        assert_eq!(replica.get_hw(), 2);

        let size = replica.get_partition_size();
        assert_eq!(size, 79);

        remove_dir_all(option.base_dir)
            .await
            .expect("delete base dir");

        let size = replica.get_partition_size();
        assert_eq!(size, 79);
    }

    /// test fetch only committed records
    #[fluvio_future::test]
    async fn test_committed_fetch() {
        let option = base_option("test_commit_fetch");

        let mut replica = create_replica("test", 0, option).await;

        let mut batch = create_batch();
        let batch_len = batch.write_size(0);
        replica
            .write_batch(&mut batch)
            .await
            .expect("writing records");

        let slice = replica
            .read_partition_slice(0, FileReplica::PREFER_MAX_LEN, Isolation::ReadCommitted)
            .await
            .expect("read");
        debug!("slice response: {:#?}", slice);
        assert_eq!(slice.file_slice.unwrap().len(), 0);

        replica
            .update_high_watermark_to_end()
            .await
            .expect("update high watermark");

        debug!(
            "replica end: {} high: {}",
            replica.get_leo(),
            replica.get_hw()
        );

        let slice = replica
            .read_partition_slice(0, FileReplica::PREFER_MAX_LEN, Isolation::ReadCommitted)
            .await
            .expect("read");
        debug!("slice response: {:#?}", slice);
        assert_eq!(slice.file_slice.unwrap().len() as usize, batch_len);

        // write 1 more batch
        let mut batch = create_batch();
        replica
            .write_batch(&mut batch)
            .await
            .expect("writing 2nd batch");
        debug!(
            "2nd batch: replica end: {} high: {}",
            replica.get_leo(),
            replica.get_hw()
        );

        let slice = replica
            .read_partition_slice(0, FileReplica::PREFER_MAX_LEN, Isolation::ReadCommitted)
            .await
            .expect("read");
        debug!("slice response: {:#?}", slice);
        // should return same records as 1 batch since we didn't commit 2nd batch
        assert_eq!(slice.file_slice.unwrap().len() as usize, batch_len);
    }

    #[fluvio_future::test]
    async fn test_replica_delete() {
        let mut option = base_option("test_delete");
        option.max_batch_size = 50; // enforce 50 length

        let replica = create_replica("testr", START_OFFSET, option.clone()).await;

        let test_file = option.base_dir.join("testr-0").join(TEST_SEG_NAME);

        assert!(test_file.exists());

        replica.remove().await.expect("removed");

        assert!(!test_file.exists());
    }

    #[fluvio_future::test]
    async fn test_replica_limit_batch() {
        let mut option = base_option("test_batch_limit");
        option.max_batch_size = 100;
        option.update_hw = false;

        let mut replica = create_replica("test", START_OFFSET, option).await;

        let mut small_batch = BatchProducer::builder().build().expect("batch").records();
        assert!(small_batch.write_size(0) < 100); // ensure we are writing less than 100 bytes
        replica
            .write_recordset(&mut small_batch, true)
            .await
            .expect("writing records");

        let mut largest_batch = BatchProducer::builder()
            .per_record_bytes(200)
            .build()
            .expect("batch")
            .records();
        assert!(largest_batch.write_size(0) > 100); // ensure we are writing more than 100
        let err = replica
            .write_recordset(&mut largest_batch, true)
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<StorageError>().expect("downcast"),
            StorageError::BatchTooBig(_)
        ));
    }

    /// create replicat with multiple segments
    #[fluvio_future::test]
    async fn test_replica_multiple_segment() {
        let mut option = base_option("test_find_segment");
        // enough for 2 batch (2 records per batch)
        option.segment_max_bytes = 160;
        option.index_max_interval_bytes = 50; // ensure we are writing to index

        let producer = BatchProducer::builder()
            .records(2u16)
            .record_generator(Arc::new(|_, _| Record::new("1")))
            .build()
            .expect("batch");

        let mut new_replica = create_replica("test", 0, option.clone()).await;
        let reader = new_replica.prev_segments.read().await;
        assert!(reader.len() == 0);
        drop(reader);

        // this will create  1 segment
        new_replica
            .write_batch(&mut producer.generate_batch())
            .await
            .expect("write");
        new_replica
            .write_batch(&mut producer.generate_batch())
            .await
            .expect("write");

        let reader = new_replica.prev_segments.read().await;
        assert_eq!(reader.len(), 0);
        assert!(reader.find_segment(0).is_none());
        assert!(reader.find_segment(1).is_none());
        drop(reader);

        // overflow, will create 2nd segment
        new_replica
            .write_batch(&mut producer.generate_batch())
            .await
            .expect("write");

        assert_eq!(new_replica.prev_segments.min_offset(), 0);
        let reader = new_replica.prev_segments.read().await;
        assert_eq!(reader.len(), 1);

        //    println!("new replica segments: {:#?}", new_replica.prev_segments);
        let first_segment = reader.get_segment(0).expect("some");
        assert_eq!(first_segment.get_base_offset(), 0);
        assert_eq!(first_segment.get_end_offset(), 4);
        assert!(reader.find_segment(0).is_some());
        drop(reader);
        drop(new_replica);

        // reload replica
        let old_replica = create_replica("test", 0, option.clone()).await;
        let reader = old_replica.prev_segments.read().await;
        //  println!("old replica segments: {:#?}", old_replica.prev_segments);
        assert!(reader.len() == 1);
        let (_, segment) = reader.find_segment(0).expect("some");

        assert_eq!(segment.get_base_offset(), 0);
        assert_eq!(segment.get_end_offset(), 4);
    }

    /// test replica with purging segments
    #[fluvio_future::test]
    async fn test_replica_segment_purge() {
        let storage_config = StorageConfig::builder()
            .cleaning_interval_ms(200)
            .build()
            .expect("build");

        let mut option = base_option("test_replica_purge");
        // enough for 2 batch (2 records per batch)
        option.segment_max_bytes = 160;
        option.index_max_interval_bytes = 50; // ensure we are writing to index
        option.retention_seconds = 1;

        let producer = BatchProducer::builder()
            .records(2u16)
            .record_generator(Arc::new(|_, _| Record::new("1")))
            .build()
            .expect("batch");

        let mut new_replica = FileReplica::create_or_load_with_storage(
            "test",
            0,
            0,
            option.clone(),
            Arc::new(storage_config),
        )
        .await
        .expect("create");
        let reader = new_replica.prev_segments.read().await;
        assert!(reader.len() == 0);
        drop(reader);

        // this will create active sgment
        new_replica
            .write_batch(&mut producer.generate_batch())
            .await
            .expect("write");
        new_replica
            .write_batch(&mut producer.generate_batch())
            .await
            .expect("write");

        // wait enough so it won't get purged
        sleep(Duration::from_millis(700)).await;

        // overflow, will create 2nd segment
        new_replica
            .write_batch(&mut producer.generate_batch())
            .await
            .expect("write");

        let reader = new_replica.prev_segments.read().await;
        assert_eq!(reader.len(), 1);
        drop(reader);

        sleep(Duration::from_millis(1000)).await; // clear should purge

        let segments = new_replica.prev_segments.clone();
        assert_eq!(Arc::strong_count(&segments), 3);
        let reader = new_replica.prev_segments.read().await;
        assert_eq!(reader.len(), 0);
        drop(reader);

        new_replica.remove().await.expect("remove");
        drop(new_replica);
        sleep(Duration::from_millis(300)).await; // clear should end
        assert_eq!(Arc::strong_count(&segments), 1);
    }

    #[fluvio_future::test]
    async fn test_replica_size_enforced() {
        //given
        let storage_config = StorageConfig::builder()
            .cleaning_interval_ms(200)
            .build()
            .expect("build");

        let mut option = base_option("test_size");
        let max_partition_size = 512;
        let max_segment_size = 128;
        option.max_partition_size = max_partition_size;
        option.segment_max_bytes = max_segment_size;

        let mut replica = FileReplica::create_or_load_with_storage(
            "test",
            0,
            START_OFFSET,
            option.clone(),
            Arc::new(storage_config),
        )
        .await
        .expect("replica created");

        let mut batch = create_batch_with_producer(12, 5);
        for i in 1..=20 {
            //at least 20*5*8 bytes to store these records
            replica.write_batch(&mut batch).await.expect("batch sent");
            assert_eq!(replica.get_leo(), START_OFFSET + 5 * i);
        }

        //when
        sleep(Duration::from_millis(1000)).await; //Cleaner should have run

        //then
        let partition_size = replica.get_partition_size();
        assert!(
            partition_size < max_partition_size,
            "replica size must not exceed max_partition_size config. Was {partition_size}"
        );
        let prev_segments = replica.prev_segments.read().await.len();
        assert_eq!(
            prev_segments, 3,
            "segments must be removed to enforce size. Was {prev_segments}"
        );
    }

    /// fix bad segment
    #[fluvio_future::test]
    async fn test_replica_repair_bad_header() {
        let option = base_option("test_replica_repair_bad_header");

        let mut replica = create_replica("test", START_OFFSET, option.clone()).await;

        // write 2 batches
        replica
            .write_batch(&mut create_batch())
            .await
            .expect("write");
        replica
            .write_batch(&mut create_batch())
            .await
            .expect("write");

        // make sure we can read
        let orig_slice = replica
            .read_all_uncommitted_records(FileReplica::PREFER_MAX_LEN)
            .await
            .expect("read");
        let orig_slice_len = orig_slice.file_slice.unwrap().len();
        info!(orig_slice_len, "original file slice len");
        drop(replica);

        let test_fs_path = option.base_dir.join("test-0").join(TEST_SEG_NAME);
        debug!("using test  logfile: {:#?}", test_fs_path);

        let original_fs_len = std::fs::metadata(&test_fs_path)
            .expect("get metadata")
            .len();

        info!(original_fs_len, "original fs len");

        let mut file = fluvio_future::fs::util::open_read_append(&test_fs_path)
            .await
            .expect("opening log file");
        // add some junk
        let bytes = vec![0x01, 0x02, 0x03];
        file.write_all(&bytes).await.expect("write some junk");
        file.flush().await.expect("flush");
        drop(file);

        let invalid_fs_len = std::fs::metadata(&test_fs_path)
            .expect("get metadata")
            .len();
        assert_eq!(invalid_fs_len, original_fs_len + 3);

        // reopen replica
        let replica2 = create_replica("test", START_OFFSET, option.clone()).await;

        // make sure we can read original batches
        let slice = replica2
            .read_all_uncommitted_records(FileReplica::PREFER_MAX_LEN)
            .await
            .expect("read");
        assert_eq!(slice.file_slice.unwrap().len(), orig_slice_len);

        drop(replica2);

        let repaird_fs_len = std::fs::metadata(&test_fs_path)
            .expect("get metadata")
            .len();
        assert_eq!(repaird_fs_len, original_fs_len);

        // reopen replica
    }
}
