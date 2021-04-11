use std::mem;

use fluvio_protocol::Encoder;
use fluvio_types::SpuId;
use tracing::{debug, trace, error, warn};
use async_trait::async_trait;

use fluvio_future::fs::{create_dir_all, remove_dir_all};
use dataplane::{ErrorCode, Isolation, Offset, ReplicaKey, Size};
use dataplane::batch::DefaultBatch;
use dataplane::record::RecordSet;

use crate::{checkpoint::CheckPoint};
use crate::range_map::SegmentList;
use crate::segment::MutableSegment;
use crate::config::ConfigOption;
use crate::SegmentSlice;
use crate::{StorageError, SlicePartitionResponse, ReplicaStorage};

/// Replica is public abstraction for commit log which are distributed.
/// Internally it is stored as list of segments.  Each segment contains finite sets of record batches.
///
#[derive(Debug)]
pub struct FileReplica {
    #[allow(dead_code)]
    last_base_offset: Offset,
    #[allow(dead_code)]
    partition: Size,
    option: ConfigOption,
    active_segment: MutableSegment,
    prev_segments: SegmentList,
    commit_checkpoint: CheckPoint<Offset>,
}

impl Unpin for FileReplica {}

fn default_config(spu_id: SpuId, config: &ConfigOption) -> ConfigOption {
    let base_dir = config.base_dir.join(format!("spu-logs-{}", spu_id));
    let new_config = config.clone();
    new_config.base_dir(base_dir)
}

#[async_trait]
impl ReplicaStorage for FileReplica {
    type Config = ConfigOption;

    async fn create(
        replica: &ReplicaKey,
        spu: SpuId,
        base_config: Self::Config,
    ) -> Result<Self, StorageError> {
        let config = default_config(spu, &base_config);
        Self::create(replica.topic.clone(), replica.partition as u32, 0, &config).await
    }

    fn get_hw(&self) -> Offset {
        *self.commit_checkpoint.get_offset()
    }

    /// offset mark that beginning of uncommitted
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

    async fn read_partition_slice<P>(
        &self,
        offset: Offset,
        max_len: u32,
        isolation: Isolation,
        partition_response: &mut P,
    ) -> (Offset, Offset)
    where
        P: SlicePartitionResponse + Send,
    {
        match isolation {
            Isolation::ReadCommitted => {
                self.read_records(offset, Some(self.get_hw()), max_len, partition_response)
                    .await
            }
            Isolation::ReadUncommitted => {
                self.read_records(offset, None, max_len, partition_response)
                    .await
            }
        }
    }

    /// write records to this replica
    /// if update_highwatermark is set, set high watermark is end
    //  this is used when LSR = 1
    async fn write_recordset(
        &mut self,
        records: &mut RecordSet,
        update_highwatermark: bool,
    ) -> Result<(), StorageError> {
        let max_batch_size = self.option.max_batch_size as usize;
        // check if any of the records's batch exceed max length
        for batch in &records.batches {
            if batch.write_size(0) > max_batch_size {
                return Err(StorageError::BatchTooBig(max_batch_size));
            }
        }

        for mut batch in &mut records.batches {
            self.write_batch(&mut batch).await?;
        }

        if update_highwatermark {
            self.update_high_watermark_to_end().await?;
        }
        Ok(())
    }

    /// update committed offset (high watermark)
    /// if true, hw is updated
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

    async fn remove(&self) -> Result<(), StorageError> {
        remove_dir_all(&self.option.base_dir)
            .await
            .map_err(|err| err.into())
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
    pub async fn create<S>(
        topic: S,
        partition: Size,
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<FileReplica, StorageError>
    where
        S: AsRef<str> + Send + 'static,
    {
        let replica_dir = option.base_dir.join(replica_dir_name(topic, partition));

        debug!("creating rep dir: {}", replica_dir.display());
        create_dir_all(&replica_dir).await?; // ensure dir_name exits

        let mut rep_option = option.clone();
        rep_option.base_dir = replica_dir;
        // create active segment

        let (segments, last_offset_res) = SegmentList::from_dir(&rep_option).await?;

        let active_segment = if let Some(last_offset) = last_offset_res {
            trace!("last segment found, validating offsets: {}", last_offset);
            let mut last_segment = MutableSegment::open_for_write(last_offset, &rep_option).await?;
            last_segment.validate().await?;
            trace!(
                "segment validated with last offset: {}",
                last_segment.get_end_offset()
            );
            last_segment
        } else {
            debug!("no segment found, creating new one");
            MutableSegment::create(base_offset, &rep_option).await?
        };

        let last_base_offset = active_segment.get_base_offset();

        let commit_checkpoint: CheckPoint<Offset> =
            CheckPoint::create(&rep_option, "replication.chk", last_base_offset).await?;

        Ok(FileReplica {
            option: rep_option,
            last_base_offset,
            partition,
            active_segment,
            prev_segments: segments,
            commit_checkpoint,
        })
    }

    /// clear the any holding directory for replica
    pub async fn clear(replica: &ReplicaKey, option: &ConfigOption) {
        let replica_dir = option
            .base_dir
            .join(replica_dir_name(&replica.topic, replica.partition as u32));

        debug!("removing dir: {}", replica_dir.display());
        if let Err(err) = remove_dir_all(&replica_dir).await {
            warn!("error trying to remove: {:#?}, err: {}", replica_dir, err);
        }
    }

    /// update high watermark to end
    pub async fn update_high_watermark_to_end(&mut self) -> Result<bool, StorageError> {
        self.update_high_watermark(self.get_leo()).await
    }

    /// find the segment that contains offsets
    /// segment could be active segment which can be written
    /// or read only segment.
    pub(crate) fn find_segment(&self, offset: Offset) -> Option<SegmentSlice> {
        trace!("finding segment for: {}", offset);
        if offset >= self.active_segment.get_base_offset() {
            trace!("active segment found for: {}", offset);
            Some(self.active_segment.to_segment_slice())
        } else {
            trace!("offset is before active, searching prev segment");
            self.prev_segments
                .find_segment(offset)
                .map(|(_, segment)| segment.to_segment_slice())
        }
    }

    /// read all uncommitted records
    #[allow(unused)]
    pub async fn read_all_uncommitted_records<P>(
        &self,
        max_len: u32,
        response: &mut P,
    ) -> (Offset, Offset)
    where
        P: SlicePartitionResponse,
    {
        self.read_records(self.get_hw(), None, max_len, response)
            .await
    }

    /// read record slice into response
    /// * `start_offset`:  start offsets
    /// * `max_offset`:  max offset (exclusive)
    /// * `responsive`:  output
    /// * `max_len`:  max length of the slice
    //  return leo, hw
    async fn read_records<P>(
        &self,
        start_offset: Offset,
        max_offset: Option<Offset>,
        max_len: u32,
        response: &mut P,
    ) -> (Offset, Offset)
    where
        P: SlicePartitionResponse,
    {
        let hw = self.get_hw();
        let leo = self.get_leo();
        debug!(
            "read records at: {}, max: max: {:#?}, hw: {}",
            start_offset, max_offset, hw,
        );

        response.set_hw(hw);
        response.set_last_stable_offset(hw);
        response.set_log_start_offset(self.get_log_start_offset());

        match self.find_segment(start_offset) {
            Some(segment) => {
                let slice = match segment {
                    SegmentSlice::MutableSegment(segment) => {
                        // optimization
                        if start_offset == self.get_leo() {
                            trace!("start offset is same as end offset, skipping");
                            return (leo, hw);
                        } else {
                            debug!(
                                "active segment with base offset: {} found for offset: {}",
                                segment.get_base_offset(),
                                start_offset
                            );
                            segment.records_slice(start_offset, max_offset).await
                        }
                    }
                    SegmentSlice::Segment(segment) => {
                        debug!(
                            "read segment with base offset: {} found for offset: {}",
                            segment.get_base_offset(),
                            start_offset
                        );
                        segment.records_slice(start_offset, max_offset).await
                    }
                };

                match slice {
                    Ok(slice) => match slice {
                        Some(slice) => {
                            use fluvio_future::file_slice::AsyncFileSlice;

                            let limited_slice = if slice.len() > max_len as u64 {
                                debug!(
                                    "retrieved record slice fd: {}, position: {}, max {} out of len {}",
                                    slice.fd(),
                                    slice.position(),
                                    max_len,
                                    slice.len()
                                );
                                AsyncFileSlice::new(slice.fd(), slice.position(), max_len as u64)
                            } else {
                                debug!(
                                    "retrieved record slice fd: {}, position: {}, len: {}",
                                    slice.fd(),
                                    slice.position(),
                                    slice.len()
                                );

                                slice
                            };

                            // limit slice
                            response.set_slice(limited_slice);
                        }
                        None => {
                            debug!("records not found for: {}", start_offset);
                            response.set_error_code(ErrorCode::OffsetOutOfRange);
                        }
                    },
                    Err(err) => {
                        response.set_error_code(ErrorCode::UnknownServerError);
                        error!("error fetch: {:#?}", err);
                    }
                }
            }
            None => {
                response.set_error_code(ErrorCode::OffsetOutOfRange);
                debug!("segment not found for offset: {}", start_offset);
            }
        }

        (leo, hw)
    }

    async fn write_batch(&mut self, item: &mut DefaultBatch) -> Result<(), StorageError> {
        trace!("start_send");
        if !(self.active_segment.write_batch(item).await?) {
            debug!("segment has no room, rolling over previous segment");
            self.active_segment.roll_over().await?;
            let last_offset = self.active_segment.get_end_offset();
            let new_segment = MutableSegment::create(last_offset, &self.option).await?;
            let old_mut_segment = mem::replace(&mut self.active_segment, new_segment);
            let old_segment = old_mut_segment.as_segment().await?;
            self.prev_segments.add_segment(old_segment);
            self.active_segment.write_batch(item).await?;
        }
        Ok(())
    }
}

// generate replication folder name
fn replica_dir_name<S: AsRef<str>>(topic_name: S, partition_index: Size) -> String {
    format!("{}-{}", topic_name.as_ref(), partition_index)
}

#[cfg(test)]
mod tests {

    use tracing::debug;
    use std::env::temp_dir;
    use std::fs;
    use std::fs::metadata;
    use std::io::Cursor;

    use fluvio_future::test_async;
    use dataplane::{Isolation, batch::DefaultBatch};
    use dataplane::{Offset, ErrorCode};
    use dataplane::core::{Decoder, Encoder};
    use dataplane::fetch::FilePartitionResponse;
    use dataplane::record::RecordSet;
    use dataplane::fixture::{BatchProducer, create_batch};
    use dataplane::fixture::read_bytes_from_file;
    use flv_util::fixture::ensure_clean_dir;

    use crate::config::ConfigOption;
    use crate::StorageError;
    use crate::ReplicaStorage;

    use super::FileReplica;

    const TEST_SEG_NAME: &str = "00000000000000000020.log";
    const TEST_SE2_NAME: &str = "00000000000000000022.log";
    const TEST_SEG_IDX: &str = "00000000000000000020.index";
    const TEST_SEG2_IDX: &str = "00000000000000000022.index";
    const START_OFFSET: Offset = 20;

    /// create option, ensure they are clean
    fn base_option(dir: &str) -> ConfigOption {
        let base_dir = temp_dir().join(dir);
        ensure_clean_dir(&base_dir);
        ConfigOption {
            segment_max_bytes: 10000,
            base_dir,
            index_max_interval_bytes: 1000,
            index_max_bytes: 1000,
            ..Default::default()
        }
    }

    fn rollover_option(dir: &str) -> ConfigOption {
        let base_dir = temp_dir().join(dir);
        ensure_clean_dir(&base_dir);
        ConfigOption {
            segment_max_bytes: 100,
            base_dir,
            index_max_bytes: 1000,
            index_max_interval_bytes: 0,
            ..Default::default()
        }
    }

    #[test_async]
    async fn test_replica_simple() -> Result<(), StorageError> {
        let option = base_option("test_simple");
        let mut replica = FileReplica::create("test", 0, START_OFFSET, &option)
            .await
            .expect("test replica");

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
        let bytes = read_bytes_from_file(&test_file)?;

        let batch = DefaultBatch::decode_from(&mut Cursor::new(bytes), 0)?;
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.get_base_offset(), START_OFFSET);
        assert_eq!(batch.get_header().last_offset_delta, 1);
        assert_eq!(batch.records().len(), 2);

        // there should not be any segment for offset 0 since base offset is 20
        let segment = replica.find_segment(0);
        assert!(segment.is_none());

        // segment with offset 20 should be active segment
        assert!(replica.find_segment(20).unwrap().is_active());
        assert!(replica.find_segment(21).unwrap().is_active());
        assert!(replica.find_segment(30).is_some()); // any higher offset should result in current segment

        Ok(())
    }

    const TEST_UNCOMMIT_DIR: &str = "test_uncommitted";

    #[test_async]
    async fn test_uncommitted_fetch() -> Result<(), StorageError> {
        let option = base_option(TEST_UNCOMMIT_DIR);

        let mut replica = FileReplica::create("test", 0, 0, &option)
            .await
            .expect("test replica");

        assert_eq!(replica.get_leo(), 0);
        assert_eq!(replica.get_hw(), 0);

        // reading empty replica should return empyt records
        let mut empty_response = FilePartitionResponse::default();
        replica
            .read_all_uncommitted_records(FileReplica::PREFER_MAX_LEN, &mut empty_response)
            .await;
        assert_eq!(empty_response.records.len(), 0);
        assert_eq!(empty_response.error_code, ErrorCode::None);

        // write batches
        let mut batch = create_batch();
        let batch_len = batch.write_size(0);
        debug!("batch len: {}", batch_len);
        replica.write_batch(&mut batch).await.expect("write");

        assert_eq!(replica.get_leo(), 2); // 2
        assert_eq!(replica.get_hw(), 0);

        // read records
        let mut partition_response = FilePartitionResponse::default();
        replica
            .read_all_uncommitted_records(FileReplica::PREFER_MAX_LEN, &mut partition_response)
            .await;
        assert_eq!(partition_response.records.len(), batch_len);

        replica.update_high_watermark(2).await?; // first batch
        assert_eq!(replica.get_hw(), 2);

        let mut batch = create_batch();
        let batch_len = batch.write_size(0);
        replica.write_batch(&mut batch).await?;

        let mut partition_response = FilePartitionResponse::default();
        replica
            .read_all_uncommitted_records(FileReplica::PREFER_MAX_LEN, &mut partition_response)
            .await;
        debug!("partiton response: {:#?}", partition_response);
        assert_eq!(partition_response.records.len(), batch_len);

        replica.write_batch(&mut create_batch()).await?;
        let mut partition_response = FilePartitionResponse::default();
        replica
            .read_all_uncommitted_records(FileReplica::PREFER_MAX_LEN, &mut partition_response)
            .await;
        assert_eq!(partition_response.records.len(), batch_len * 2);

        let mut partition_response = FilePartitionResponse::default();
        replica
            .read_all_uncommitted_records(50, &mut partition_response)
            .await;
        assert_eq!(partition_response.records.len(), 50);

        Ok(())
    }

    const TEST_OFFSET_DIR: &str = "test_offset";

    #[test_async]
    async fn test_replica_end_offset() -> Result<(), StorageError> {
        let option = base_option(TEST_OFFSET_DIR);

        let mut rep_sink = FileReplica::create("test", 0, START_OFFSET, &option)
            .await
            .expect("test replica");
        rep_sink.write_batch(&mut create_batch()).await?;
        rep_sink.write_batch(&mut create_batch()).await?;
        drop(rep_sink);

        // open replica
        let replica2 = FileReplica::create("test", 0, START_OFFSET, &option)
            .await
            .expect("test replica");
        assert_eq!(replica2.get_leo(), START_OFFSET + 4);

        Ok(())
    }

    const TEST_REPLICA_DIR: &str = "test_replica";

    // you can show log by:  RUST_LOG=commit_log=debug cargo test roll_over

    #[test_async]
    async fn test_rep_log_roll_over() -> Result<(), StorageError> {
        let option = rollover_option(TEST_REPLICA_DIR);

        let mut replica = FileReplica::create("test", 1, START_OFFSET, &option)
            .await
            .expect("create rep");

        // first batch
        debug!(">>>> sending first batch");
        let mut batches = create_batch();
        replica.write_batch(&mut batches).await?;

        // second batch
        debug!(">>>> sending second batch. this should rollover");
        let mut batches = create_batch();
        replica.write_batch(&mut batches).await?;
        debug!("finish sending next batch");

        assert_eq!(replica.get_log_start_offset(), START_OFFSET);
        let replica_dir = &option.base_dir.join("test-1");
        let dir_contents = fs::read_dir(&replica_dir)?;
        assert_eq!(dir_contents.count(), 5, "should be 5 files");

        let seg2_file = replica_dir.join(TEST_SE2_NAME);
        let bytes = read_bytes_from_file(&seg2_file)?;

        let batch = DefaultBatch::decode_from(&mut Cursor::new(bytes), 0)?;
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records().len(), 2);
        assert_eq!(batch.get_base_offset(), 22);

        let metadata_res = metadata(replica_dir.join(TEST_SEG2_IDX));
        assert!(metadata_res.is_ok());
        let metadata2 = metadata_res.unwrap();
        assert_eq!(metadata2.len(), 1000);

        let seg1_metadata = metadata(replica_dir.join(TEST_SEG_IDX))?;
        assert_eq!(seg1_metadata.len(), 8);

        Ok(())
    }

    const TEST_COMMIT_DIR: &str = "test_commit";

    #[test_async]
    async fn test_replica_commit() -> Result<(), StorageError> {
        let option = base_option(TEST_COMMIT_DIR);
        let mut replica = FileReplica::create("test", 0, 0, &option)
            .await
            .expect("test replica");

        let mut records = RecordSet::default().add(create_batch());

        replica.write_recordset(&mut records, true).await?;

        // record contains 2 batch
        assert_eq!(replica.get_hw(), 2);

        drop(replica);

        // restore replica
        let replica = FileReplica::create("test", 0, 0, &option)
            .await
            .expect("test replica");
        assert_eq!(replica.get_hw(), 2);

        Ok(())
    }

    const TEST_COMMIT_FETCH_DIR: &str = "test_commit_fetch";

    /// test fetch only committed records
    #[test_async]
    async fn test_committed_fetch() -> Result<(), StorageError> {
        let option = base_option(TEST_COMMIT_FETCH_DIR);

        let mut replica = FileReplica::create("test", 0, 0, &option)
            .await
            .expect("test replica");

        let mut batch = create_batch();
        let batch_len = batch.write_size(0);
        replica
            .write_batch(&mut batch)
            .await
            .expect("writing records");

        let mut partition_response = FilePartitionResponse::default();
        replica
            .read_partition_slice(
                0,
                FileReplica::PREFER_MAX_LEN,
                Isolation::ReadCommitted,
                &mut partition_response,
            )
            .await;
        debug!("partition response: {:#?}", partition_response);
        assert_eq!(partition_response.records.len(), 0);

        replica
            .update_high_watermark_to_end()
            .await
            .expect("update high watermark");

        debug!(
            "replica end: {} high: {}",
            replica.get_leo(),
            replica.get_hw()
        );

        let mut partition_response = FilePartitionResponse::default();
        replica
            .read_partition_slice(
                0,
                FileReplica::PREFER_MAX_LEN,
                Isolation::ReadCommitted,
                &mut partition_response,
            )
            .await;
        debug!("partition response: {:#?}", partition_response);
        assert_eq!(partition_response.records.len(), batch_len);

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

        let mut partition_response = FilePartitionResponse::default();
        replica
            .read_partition_slice(
                0,
                FileReplica::PREFER_MAX_LEN,
                Isolation::ReadCommitted,
                &mut partition_response,
            )
            .await;
        debug!("partition response: {:#?}", partition_response);
        // should return same records as 1 batch since we didn't commit 2nd batch
        assert_eq!(partition_response.records.len(), batch_len);

        Ok(())
    }

    #[test_async]
    async fn test_replica_delete() -> Result<(), StorageError> {
        let mut option = base_option("test_delete");
        option.max_batch_size = 50; // enforce 50 length

        let replica = FileReplica::create("testr", 0, START_OFFSET, &option)
            .await
            .expect("test replica");

        let test_file = option.base_dir.join("testr-0").join(TEST_SEG_NAME);

        assert!(test_file.exists());

        replica.remove().await.expect("removed");

        assert!(!test_file.exists());

        Ok(())
    }

    #[test_async]
    async fn test_replica_limit_batch() -> Result<(), StorageError> {
        let mut option = base_option("test_batch_limit");
        option.max_batch_size = 100;
        option.update_hw = false;

        let mut replica = FileReplica::create("test", 0, START_OFFSET, &option)
            .await
            .expect("test replica");

        let mut small_batch = BatchProducer::builder().build().expect("batch").records();
        assert!(small_batch.write_size(0) < 100); // ensure we are writing less than 100 bytes
        replica
            .write_recordset(&mut small_batch, true)
            .await
            .expect("writing records");

        let mut larget_batch = BatchProducer::builder()
            .per_record_bytes(200)
            .build()
            .expect("batch")
            .records();
        assert!(larget_batch.write_size(0) > 100); // ensure we are writing more than 100
        assert!(matches!(
            replica
                .write_recordset(&mut larget_batch, true)
                .await
                .unwrap_err(),
            StorageError::BatchTooBig(_)
        ));

        Ok(())
    }
}
