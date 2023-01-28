use std::fmt;
use std::io::Error as IoError;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, trace, instrument, info, error};
use anyhow::{Result};

use fluvio_future::fs::remove_file;
use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_protocol::record::{Batch, BatchRecords};
use fluvio_protocol::record::{Offset, Size, Size64};
use fluvio_protocol::link::ErrorCode;

use crate::batch_header::{BatchHeaderStream, FileEmptyRecords};
use crate::mut_index::MutLogIndex;
use crate::index::LogIndex;
use crate::index::Index;
use crate::records::FileRecords;
use crate::mut_records::MutFileRecords;
use crate::records::FileRecordsSlice;
use crate::config::{SharedReplicaConfig};
use crate::StorageError;
use crate::batch::{FileBatchStream};
use crate::index::OffsetPosition;
use crate::validator::LogValidationError;

pub type MutableSegment = Segment<MutLogIndex, MutFileRecords>;
pub type ReadSegment = Segment<LogIndex, FileRecordsSlice>;

pub(crate) struct BatchPosition {
    batch: Batch<FileEmptyRecords>,
    pos: Size,
}

/// Segment contains both message log and index
pub struct Segment<I, L> {
    option: Arc<SharedReplicaConfig>,
    msg_log: L,
    index: I,
    base_offset: Offset,
    end_offset: Offset,
}

impl<I, L> fmt::Debug for Segment<I, L> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Segment(base={},end={})",
            self.get_base_offset(),
            self.get_end_offset()
        )
    }
}

impl<I, L> Segment<I, L> {
    /// end offset, this always starts as baseoffset which indicates empty records
    pub fn get_end_offset(&self) -> Offset {
        self.end_offset
    }

    #[cfg(test)]
    /// set end offset, this is used by test
    pub(crate) fn set_end_offset(&mut self, offset: Offset) {
        self.end_offset = offset;
    }

    pub fn get_base_offset(&self) -> Offset {
        self.base_offset
    }
}

impl<I, L> Segment<I, L>
where
    I: Index,
    I: Deref<Target = [(Size, Size)]>,
    L: FileRecords,
{
    #[allow(dead_code)]
    pub fn get_index(&self) -> &I {
        &self.index
    }

    pub fn get_msg_log(&self) -> &L {
        &self.msg_log
    }

    pub async fn open_batch_header_stream(
        &self,
        start_pos: Size,
    ) -> Result<BatchHeaderStream, StorageError> {
        trace!(
            path = ?self.msg_log.get_path(),
            start_pos,
            "opening batch header"
        );

        // let metadata = file.metadata().await?;
        // debug!("batch file len: {}",metadata.len());
        let mut stream = BatchHeaderStream::open(self.msg_log.get_path()).await?;
        stream.set_absolute(start_pos).await?;
        Ok(stream)
    }

    #[allow(dead_code)]
    pub async fn open_default_batch_stream(&self) -> Result<FileBatchStream, StorageError> {
        let file_path = self.msg_log.get_path();
        debug!("opening batch stream: {:#?}", file_path);
        // let file = file_util::open(file_path).await?;
        Ok(FileBatchStream::open(&file_path).await?)
    }

    /// get file slice from offset to end of segment
    #[instrument(skip(self))]
    pub async fn records_slice(
        &self,
        start_offset: Offset,
        max_offset_opt: Option<Offset>,
    ) -> Result<Option<AsyncFileSlice>, ErrorCode> {
        match self
            .find_offset_position(start_offset)
            .await
            .map_err(|err| ErrorCode::Other(format!("offset error: {err:#?}")))?
        {
            Some(start_pos) => {
                let batch = start_pos.batch;
                let pos = start_pos.pos;
                debug!(
                    batch_offset = batch.base_offset,
                    batch_len = batch.batch_len,
                    "found start pos",
                );
                match max_offset_opt {
                    Some(max_offset) => {
                        // max_offset comes from HW, which could be greater than current segment end.
                        let effective_max_offset = std::cmp::min(max_offset, self.get_end_offset());
                        // check if max offset same as segment end
                        if effective_max_offset == self.get_end_offset() {
                            debug!("effective max offset is same as end offset, reading to end");
                            Ok(Some(self.msg_log.as_file_slice(pos).map_err(|err| {
                                ErrorCode::Other(format!("msg as file slice: {err:#?}"))
                            })?))
                        } else {
                            debug!(effective_max_offset, max_offset);
                            match self
                                .find_offset_position(effective_max_offset)
                                .await
                                .map_err(|err| {
                                    ErrorCode::Other(format!("offset error: {err:#?}"))
                                })? {
                                Some(end_pos) => Ok(Some(
                                    self.msg_log
                                        .as_file_slice_from_to(pos, end_pos.pos - pos)
                                        .map_err(|err| {
                                            ErrorCode::Other(format!("msg slice: {err:#?}"))
                                        })?,
                                )),
                                None => Err(ErrorCode::Other(format!(
                                    "max offset position: {effective_max_offset} not found"
                                ))),
                            }
                        }
                    }
                    None => Ok(Some(self.msg_log.as_file_slice(start_pos.pos).map_err(
                        |err| ErrorCode::Other(format!("msg slice error: {err:#?}")),
                    )?)),
                }
            }
            None => {
                debug!(start_offset, "offset position not found");
                Ok(None)
            }
        }
    }

    /// find position of the offset
    #[instrument(skip(self))]
    pub(crate) async fn find_offset_position(
        &self,
        offset: Offset,
    ) -> Result<Option<BatchPosition>> {
        debug!(offset, "trying to find offset position");
        if offset < self.base_offset {
            debug!(self.base_offset, "invalid, offset is less than base offset",);
            return Ok(None);
        }
        if offset >= self.end_offset {
            debug!(
                self.end_offset,
                "invalid,end offset is greater than end offset"
            );
            return Ok(None);
        }

        let delta = (offset - self.base_offset) as Size;
        let position = match self.index.find_offset(delta) {
            None => {
                debug!(delta, "relative offset not found in index");
                0
            }
            Some(entry) => entry.position(),
        };
        debug!(file_position = position, "found file pos");

        let mut header_stream = self.open_batch_header_stream(position).await?;
        while let Some(batch_pos) = header_stream.try_next().await? {
            let pos = batch_pos.get_pos();
            let batch = batch_pos.inner();

            trace!(
                base_offset = batch.base_offset,
                batch_len = batch.batch_len,
                last_offset = batch.get_last_offset(),
                "batch_pos"
            );
            let last_offset = batch.get_last_offset();
            if last_offset >= offset {
                debug!(last_offset, "found batch last offset");
                return Ok(Some(BatchPosition { batch, pos }));
            } else {
                trace!(last_offset, "skipping batch end offset");
            }
        }

        Ok(None)
    }

    pub(crate) fn occupied_memory(&self) -> Size64 {
        self.index.len() + self.msg_log.len()
    }
}

impl Segment<LogIndex, FileRecordsSlice> {
    /// open read only segments if base and end offset are known
    pub async fn open_for_read(
        base_offset: Offset,
        end_offset: Offset,
        option: Arc<SharedReplicaConfig>,
    ) -> Result<Self> {
        debug!(base_offset, end_offset, ?option, "open for read");
        let msg_log = FileRecordsSlice::open(base_offset, option.clone()).await?;
        let base_offset = msg_log.get_base_offset();
        debug!(base_offset, end_offset, "offset from msg log");
        let index = LogIndex::open_from_offset(base_offset, option.clone()).await?;

        Ok(Segment {
            msg_log,
            index,
            option,
            base_offset,
            end_offset,
        })
    }

    /// open read only segments if we don't know end offset
    #[instrument(skip(option),fields(base_dir=?option.base_dir))]
    pub async fn open_unknown(
        base_offset: Offset,
        option: Arc<SharedReplicaConfig>,
    ) -> Result<Self> {
        let msg_log = FileRecordsSlice::open(base_offset, option.clone()).await?;
        let index = LogIndex::open_from_offset(base_offset, option.clone()).await?;
        let base_offset = msg_log.get_base_offset();
        match msg_log.validate(&index).await {
            Ok(val) => {
                // check if validation is successful
                if let Some(err) = val.error {
                    error!(err = ?err, "segment validation failed");
                    return Err(err.into());
                }

                info!(end_offset = val.leo(), base_offset = val.base_offset, time_ms = %val.duration.as_millis(), "segment validated");
                Ok(Segment {
                    msg_log,
                    index,
                    option,
                    base_offset,
                    end_offset: val.leo(),
                })
            }
            Err(err) => {
                error!(?err, "segment validation encountered fail error");
                Err(err)
            }
        }
    }

    pub(crate) fn is_expired(&self, expired_duration: &Duration) -> bool {
        self.msg_log.is_expired(expired_duration)
    }

    pub(crate) async fn remove(self) -> Result<(), StorageError> {
        self.msg_log.remove().await?;
        let index_file_path = self.index.clean();
        info!(index_path = %index_file_path.display(),"removing index file");
        remove_file(&index_file_path).await?;
        Ok(())
    }
}

/// Implementation for Active segment
impl Segment<MutLogIndex, MutFileRecords> {
    // create segment on base directory

    pub async fn create(
        base_offset: Offset,
        option: Arc<SharedReplicaConfig>,
    ) -> Result<MutableSegment, StorageError> {
        debug!(base_offset, "creating new active segment");
        let msg_log = MutFileRecords::create(base_offset, option.clone()).await?;

        let index = MutLogIndex::create(base_offset, option.clone()).await?;

        Ok(MutableSegment {
            option: option.to_owned(),
            msg_log,
            index,
            base_offset,
            end_offset: base_offset,
        })
    }

    pub async fn open_for_write(
        base_offset: Offset,
        option: Arc<SharedReplicaConfig>,
    ) -> Result<MutableSegment, StorageError> {
        debug!(
            base_offset,
            base_dir = ?option.base_dir,
            "opening active segment for write"
        );
        let msg_log = MutFileRecords::create(base_offset, option.clone()).await?;
        let base_offset = msg_log.get_base_offset();
        let index = MutLogIndex::open(base_offset, option.clone()).await?;

        let base_offset = msg_log.get_base_offset();
        Ok(MutableSegment {
            option,
            msg_log,
            index,
            base_offset,
            end_offset: base_offset,
        })
    }

    #[cfg(test)]
    fn get_log_pos(&self) -> u32 {
        self.msg_log.get_pos()
    }

    /// validate and repair if necessary
    pub async fn validate_and_repair(&mut self) -> Result<Offset> {
        let validation = self.msg_log.validate(&self.index).await?;
        let leo = validation.leo();
        // check for error and see if it's recoverable
        if let Some(err) = validation.error {
            error!(err = ?err, "log validation failed");
            match err {
                LogValidationError::BatchDecoding(_header_error) => {
                    info!(
                        len = validation.last_valid_file_pos,
                        "batch decoding error, trying to recover"
                    );
                    // for decoding batch error, we can readjust
                    self.msg_log.set_len(validation.last_valid_file_pos).await?;
                    info!(
                        len = validation.last_valid_file_pos,
                        "readjust segment length"
                    );
                }
                _ => {
                    // for other error, we can't recover
                    return Err(err.into());
                }
            }
        }
        self.end_offset = leo;
        Ok(self.end_offset)
    }

    // shrink index
    #[cfg(test)]
    async fn shrink_index(&mut self) -> Result<(), IoError> {
        self.index.shrink().await
    }

    // perform any action during roll over
    pub async fn roll_over(&mut self) -> Result<(), IoError> {
        self.index.shrink().await
    }

    /// convert to immutable segment
    #[allow(clippy::wrong_self_convention)]
    pub async fn as_segment(self) -> Result<ReadSegment> {
        Segment::open_for_read(self.get_base_offset(), self.end_offset, self.option.clone()).await
    }

    /// use only in test
    #[cfg(test)]
    pub async fn convert_to_segment(mut self) -> Result<ReadSegment> {
        self.shrink_index().await?;
        Segment::open_for_read(self.get_base_offset(), self.end_offset, self.option.clone()).await
    }

    /// Append new batch to segment.  This will update the index and msg log
    /// This will perform following steps:
    /// 1. Set batch's base offset to current end offset
    /// 2. Append batch to msg log
    /// 3. Write batch location to index
    #[instrument(skip(batch))]
    pub async fn append_batch<R: BatchRecords>(&mut self, batch: &mut Batch<R>) -> Result<bool> {
        // adjust base offset and offset delta
        // reject if batch len is 0
        if batch.records_len() == 0 {
            return Err(StorageError::EmptyBatch.into());
        }

        batch.set_base_offset(self.end_offset);

        let next_end_offset = batch.get_last_offset();

        // relative offset of the batch to segment
        let relative_offset_in_segment = (self.end_offset - self.base_offset) as i32;
        let start_file_pos = self.msg_log.get_pos();
        debug!(
            base_offset = batch.get_base_offset(),
            current_end_offset = self.end_offset,
            next_end_offset,
            relative_offset_in_segment,
            start_file_pos,
            "writing batch",
        );

        let (write_success, batch_len, end_file_pos) = self.msg_log.write_batch(batch).await?;
        debug!(
            write_success,
            batch_len, end_file_pos, next_end_offset, "batch written"
        );
        if write_success {
            self.index
                .write_index(
                    relative_offset_in_segment as u32,
                    start_file_pos,
                    batch_len as u32,
                )
                .await?;
            self.end_offset = next_end_offset + 1;
            debug!(end_offset = self.end_offset, "updated leo");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[allow(unused)]
    pub async fn flush(&mut self) -> Result<(), StorageError> {
        self.msg_log.flush().await.map_err(|err| err.into())
    }
}

#[cfg(test)]
mod tests {

    use tracing::debug;
    use std::env::temp_dir;
    use std::fs::metadata;
    use std::io::Cursor;
    use std::path::PathBuf;

    use flv_util::fixture::ensure_new_dir;
    use fluvio_protocol::record::{Batch, MemoryRecords};
    use fluvio_protocol::record::Size;
    use fluvio_protocol::Decoder;
    use fluvio_protocol::fixture::create_batch_with_producer;
    use fluvio_protocol::fixture::create_batch;
    use fluvio_protocol::fixture::read_bytes_from_file;

    use super::MutableSegment;

    use crate::config::ReplicaConfig;
    use crate::index::OffsetPosition;

    // TODO: consolidate

    fn default_option(base_dir: PathBuf, index_max_interval_bytes: Size) -> ReplicaConfig {
        ReplicaConfig {
            segment_max_bytes: 1000,
            base_dir,
            index_max_interval_bytes,
            index_max_bytes: 1000,
            ..Default::default()
        }
    }

    const TEST_FILE_NAME: &str = "00000000000000000020.log"; // offset 20 different from other test
    const SEG_INDEX: &str = "00000000000000000020.index";

    #[fluvio_future::test]
    async fn test_segment_single_record() {
        let test_dir = temp_dir().join("seg-single-record");
        ensure_new_dir(&test_dir).expect("dir");

        let option = default_option(test_dir.clone(), 0).shared();

        let base_offset = 20;

        let mut active_segment = MutableSegment::create(base_offset, option)
            .await
            .expect("create");
        assert_eq!(active_segment.get_end_offset(), 20);

        // batch of 1
        active_segment
            .append_batch(&mut create_batch_with_producer(100, 1))
            .await
            .expect("write");
        assert_eq!(active_segment.get_end_offset(), 21);

        // check to see if batch is written
        let bytes = read_bytes_from_file(test_dir.join(TEST_FILE_NAME)).expect("read bytes");
        debug!("read {} bytes", bytes.len());

        // read batches from raw bytes to see if it can be parsed
        let batch =
            Batch::<MemoryRecords>::decode_from(&mut Cursor::new(bytes), 0).expect("decode");
        assert_eq!(batch.get_base_offset(), 20);
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records().len(), 1);

        let seg1_metadata = metadata(test_dir.join(SEG_INDEX)).expect("read metadata");
        assert_eq!(seg1_metadata.len(), 1000);

        // this should return none since we are trying find offset before start offset
        assert!((active_segment
            .find_offset_position(10)
            .await
            .expect("offset"))
        .is_none());
        let offset_position =
            (active_segment.find_offset_position(20).await.expect("find")).expect("offset exists");
        assert_eq!(offset_position.batch.get_base_offset(), 20);
        assert_eq!(offset_position.pos, 0); //
        assert_eq!(offset_position.batch.batch_len, 58);
        assert!((active_segment.find_offset_position(30).await.expect("find")).is_none());
    }

    #[fluvio_future::test]
    async fn test_segment_multiple_record() {
        let test_dir = temp_dir().join("seg-multiple-record");
        ensure_new_dir(&test_dir).expect("new");

        let option = default_option(test_dir.clone(), 0).shared();

        let base_offset = 20;

        let mut active_segment = MutableSegment::create(base_offset, option)
            .await
            .expect("segment");

        active_segment
            .append_batch(&mut create_batch_with_producer(100, 4))
            .await
            .expect("batch");

        // each record contains 9 bytes

        // check to see if batch is written
        let bytes = read_bytes_from_file(test_dir.join(TEST_FILE_NAME)).expect("read");
        debug!("read {} bytes", bytes.len());

        let batch =
            Batch::<MemoryRecords>::decode_from(&mut Cursor::new(bytes), 0).expect("decode");
        assert_eq!(batch.get_base_offset(), 20);
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records().len(), 4);

        let seg1_metadata = metadata(test_dir.join(SEG_INDEX)).expect("join");
        assert_eq!(seg1_metadata.len(), 1000);

        assert!((active_segment.find_offset_position(10).await.expect("find")).is_none());
        let offset_position =
            (active_segment.find_offset_position(20).await.expect("find")).expect("offset exists");
        assert_eq!(offset_position.batch.get_base_offset(), 20);
        assert_eq!(offset_position.pos, 0); //
        assert_eq!(offset_position.batch.batch_len, 85);
        assert!((active_segment.find_offset_position(30).await.expect("find")).is_none());
    }

    const TEST2_FILE_NAME: &str = "00000000000000000040.log"; // offset 20 different from other test

    #[fluvio_future::test]
    async fn test_segment_multiple_batch() {
        let test_dir = temp_dir().join("multiple-segment");
        ensure_new_dir(&test_dir).expect("new");

        let base_offset = 40;

        let option = default_option(test_dir.clone(), 50).shared();

        let mut seg_sink = MutableSegment::create(base_offset, option)
            .await
            .expect("write");
        seg_sink
            .append_batch(&mut create_batch())
            .await
            .expect("write");
        seg_sink
            .append_batch(&mut create_batch())
            .await
            .expect("write");
        seg_sink
            .append_batch(&mut create_batch())
            .await
            .expect("write");

        assert_eq!(seg_sink.get_end_offset(), 46);

        assert_eq!(seg_sink.get_log_pos(), 237); // each takes 79 bytes

        let index = seg_sink.get_index();
        assert_eq!(index[0].to_be(), (2, 79));

        let bytes = read_bytes_from_file(test_dir.join(TEST2_FILE_NAME)).expect("read");
        debug!("read {} bytes", bytes.len());

        let cursor = &mut Cursor::new(bytes);
        let batch = Batch::<MemoryRecords>::decode_from(cursor, 0).expect("decode");
        assert_eq!(batch.get_base_offset(), 40);
        assert_eq!(batch.get_header().last_offset_delta, 1);

        let batch2 = Batch::<MemoryRecords>::decode_from(cursor, 0).expect("decode");
        assert_eq!(batch2.get_base_offset(), 42);
        assert_eq!(batch2.get_header().last_offset_delta, 1);

        let offset_pos1 = seg_sink
            .find_offset_position(40)
            .await
            .expect("pos")
            .unwrap();
        assert_eq!(offset_pos1.batch.get_base_offset(), 40);
        assert_eq!(offset_pos1.pos, 0);
        assert_eq!(offset_pos1.batch.batch_len, 67);
        let offset_pos2 = seg_sink
            .find_offset_position(42)
            .await
            .expect("pos")
            .unwrap();
        assert_eq!(offset_pos2.batch.get_base_offset(), 42);
        assert_eq!(offset_pos2.pos, 79);
        assert_eq!(offset_pos2.batch.batch_len, 67);

        let offset_pos3 = seg_sink
            .find_offset_position(44)
            .await
            .expect("pos")
            .unwrap();
        assert_eq!(offset_pos3.batch.get_base_offset(), 44);
        assert_eq!(offset_pos3.pos, 158);
        assert_eq!(offset_pos3.batch.batch_len, 67);

        // test whether you can send batch with zero
        assert_eq!(seg_sink.get_end_offset(), 46);
        let mut next_batch = create_batch();
        next_batch.base_offset = 0;
        assert!(seg_sink.append_batch(&mut next_batch).await.is_ok());
        assert_eq!(seg_sink.get_end_offset(), 48);

        // test batch with other base offset
        let mut next_batch = create_batch();
        next_batch.base_offset = 1000;
        assert!(seg_sink.append_batch(&mut next_batch).await.is_ok());
        assert_eq!(seg_sink.get_end_offset(), 50);

        let _records = seg_sink
            .records_slice(44, Some(52))
            .await
            .expect(
                "failed to get records using max offset larger than current end offset in segment",
            )
            .expect("failed to get records");
    }
}
