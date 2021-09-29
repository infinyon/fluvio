use std::fmt;
use std::io::Error as IoError;
use std::ops::Deref;

use tracing::{debug, trace, instrument};

use dataplane::batch::Batch;
use dataplane::{Offset, Size};
use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_future::fs::util as file_util;

use crate::batch_header::{BatchHeaderStream, BatchHeaderPos};
use crate::mut_index::MutLogIndex;
use crate::index::LogIndex;
use crate::index::Index;
use crate::records::FileRecords;
use crate::mut_records::MutFileRecords;
use crate::records::FileRecordsSlice;
use crate::config::ConfigOption;
use crate::StorageError;
use crate::batch::FileBatchStream;
use crate::index::OffsetPosition;
use crate::validator::LogValidationError;
use crate::util::OffsetError;

pub type MutableSegment = Segment<MutLogIndex, MutFileRecords>;
pub type ReadSegment = Segment<LogIndex, FileRecordsSlice>;

pub enum SegmentSlice<'a> {
    MutableSegment(&'a MutableSegment),
    Segment(&'a ReadSegment),
}

impl<'a> Unpin for SegmentSlice<'a> {}

impl<'a> SegmentSlice<'a> {
    pub fn new_mut_segment(segment: &'a MutableSegment) -> Self {
        SegmentSlice::MutableSegment(segment)
    }

    pub fn new_segment(segment: &'a ReadSegment) -> Self {
        SegmentSlice::Segment(segment)
    }

    #[allow(unused)]
    pub fn is_active(&'a self) -> bool {
        match self {
            Self::MutableSegment(_) => true,
            Self::Segment(_) => false,
        }
    }
}

/// Segment contains both message log and index
pub struct Segment<I, L> {
    option: ConfigOption,
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
    /// end offset, this always starts at 0 which indicates empty records
    pub fn get_end_offset(&self) -> Offset {
        self.end_offset
    }

    #[allow(dead_code)]
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

    pub async fn open_batch_header_stream(
        &self,
        start_pos: Size,
    ) -> Result<BatchHeaderStream, StorageError> {
        trace!(
            "opening batch header: {:#?} at: {}",
            self.msg_log.get_path(),
            start_pos
        );
        let file = file_util::open(self.msg_log.get_path()).await?;
        // let metadata = file.metadata().await?;
        // debug!("batch file len: {}",metadata.len());
        BatchHeaderStream::new_with_pos(file, start_pos).await
    }

    #[allow(dead_code)]
    pub async fn open_default_batch_stream(&self) -> Result<FileBatchStream, StorageError> {
        let file_path = self.msg_log.get_path();
        debug!("opening batch stream: {:#?}", file_path);
        let file = file_util::open(file_path).await?;
        Ok(FileBatchStream::new(file))
    }

    /// get file slice from offset to end of segment
    pub async fn records_slice(
        &self,
        start_offset: Offset,
        max_offset_opt: Option<Offset>,
    ) -> Result<Option<AsyncFileSlice>, StorageError> {
        trace!("record slice at: {}", start_offset);
        match self.find_offset_position(start_offset).await? {
            Some(start_pos) => {
                trace!(
                    "found batch: {:#?} at: {}",
                    start_pos.get_batch(),
                    start_pos.get_pos()
                );
                match max_offset_opt {
                    Some(max_offset) => {
                        // check if max offset same as segment end
                        if max_offset == self.get_end_offset() {
                            trace!("max offset is same as end offset, reading to end");
                            Ok(Some(self.msg_log.as_file_slice(start_pos.get_pos())?))
                        } else {
                            trace!("end offset is supplied: {}", max_offset);
                            match self.find_offset_position(max_offset).await? {
                                Some(end_pos) => Ok(Some(self.msg_log.as_file_slice_from_to(
                                    start_pos.get_pos(),
                                    end_pos.get_pos() - start_pos.get_pos(),
                                )?)),
                                None => Err(StorageError::Offset(OffsetError::NotExistent)),
                            }
                        }
                    }
                    None => Ok(Some(self.msg_log.as_file_slice(start_pos.get_pos())?)),
                }
            }
            None => Ok(None),
        }
    }

    /// find position of the offset
    pub(crate) async fn find_offset_position(
        &self,
        offset: Offset,
    ) -> Result<Option<BatchHeaderPos>, StorageError> {
        trace!("finding offset position: {}", offset);
        if offset < self.base_offset {
            trace!(
                "invalid offset: {} is below base offset: {}",
                offset,
                self.base_offset
            );
            return Ok(None);
        }
        if offset >= self.end_offset {
            trace!(
                "invalid offset: {} exceed end offset: {}",
                offset,
                self.end_offset
            );
            return Ok(None);
        }

        let delta = (offset - self.base_offset) as Size;
        let position = match self.index.find_offset(delta) {
            None => 0,
            Some(entry) => entry.position(),
        };
        trace!("found relative pos: {}", position);

        let mut header_stream = self.open_batch_header_stream(position).await?;
        while let Some(batch_pos) = header_stream.next().await {
            let last_offset = batch_pos.get_last_offset();
            if last_offset >= offset {
                trace!(
                    "found batch last offset which matches offset: {}",
                    last_offset
                );
                return Ok(Some(batch_pos));
            } else {
                trace!("skipping batch end offset: {}", last_offset);
            }
        }
        Ok(None)
    }
}

impl Segment<LogIndex, FileRecordsSlice> {
    pub async fn open_for_read(
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<Self, StorageError> {
        let msg_log = FileRecordsSlice::open(base_offset, option).await?;
        let base_offset = msg_log.get_base_offset();
        let index = LogIndex::open_from_offset(base_offset, option).await?;

        let base_offset = msg_log.get_base_offset();
        Ok(Segment {
            msg_log,
            index,
            option: option.to_owned(),
            base_offset,
            end_offset: base_offset,
        })
    }

    pub fn to_segment_slice(&self) -> SegmentSlice {
        SegmentSlice::new_segment(self)
    }
}

impl Unpin for Segment<MutLogIndex, MutFileRecords> {}

/// Implementation for Active segment
impl Segment<MutLogIndex, MutFileRecords> {
    // create segment on base directory

    pub async fn create(
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<MutableSegment, StorageError> {
        debug!(base_offset, "creating new active segment");
        let msg_log = MutFileRecords::create(base_offset, option).await?;

        let index = MutLogIndex::create(base_offset, option).await?;

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
        option: &ConfigOption,
    ) -> Result<MutableSegment, StorageError> {
        trace!(
            "opening mut segment: {} at: {:#?}",
            base_offset,
            &option.base_dir
        );
        let msg_log = MutFileRecords::open(base_offset, option).await?;
        let base_offset = msg_log.get_base_offset();
        let index = MutLogIndex::open(base_offset, option).await?;

        let base_offset = msg_log.get_base_offset();
        Ok(MutableSegment {
            option: option.to_owned(),
            msg_log,
            index,
            base_offset,
            end_offset: base_offset,
        })
    }

    fn get_log_pos(&self) -> u32 {
        self.msg_log.get_pos()
    }

    /// validate the segment and load last offset
    pub async fn validate(&mut self) -> Result<(), StorageError> {
        self.end_offset = self.msg_log.validate().await?;
        Ok(())
    }

    // shrink index
    async fn shrink_index(&mut self) -> Result<(), IoError> {
        self.index.shrink().await
    }

    // perform any action during roll over
    pub async fn roll_over(&mut self) -> Result<(), IoError> {
        self.index.shrink().await
    }

    /// convert to immutable segment
    #[allow(clippy::wrong_self_convention)]
    pub async fn as_segment(self) -> Result<ReadSegment, StorageError> {
        Segment::open_for_read(self.get_base_offset(), &self.option).await
    }

    /// shrink and convert as immutable
    #[allow(dead_code)]
    pub async fn convert_to_segment(mut self) -> Result<ReadSegment, StorageError> {
        self.shrink_index().await?;
        Segment::open_for_read(self.get_base_offset(), &self.option).await
    }

    pub fn to_segment_slice(&self) -> SegmentSlice {
        SegmentSlice::new_mut_segment(self)
    }

    #[instrument(skip(item))]
    pub async fn write_batch(&mut self, item: &mut Batch) -> Result<bool, StorageError> {
        let current_offset = self.end_offset;
        let base_offset = self.base_offset;
        let pos = self.get_log_pos();

        // fill in the base offset using current offset if record's batch offset is 0
        // ensure batch is not already recorded
        if item.base_offset == 0 {
            item.set_base_offset(current_offset);
        } else if item.base_offset < current_offset {
            return Err(StorageError::LogValidation(
                LogValidationError::ExistingBatch,
            ));
        }

        let batch_offset_delta = (current_offset - base_offset) as i32;
        debug!(
            base_offset,
            file_offset = pos,
            batch_len = compute_batch_record_size(item),
            "start writing batch"
        );

        if self.msg_log.write_batch(item).await? {
            let batch_len = self.msg_log.get_pos();

            self.index
                .write_index((batch_offset_delta as u32, pos, batch_len))
                .await?;

            let last_offset_delta = self.msg_log.get_item_last_offset_delta();
            trace!("flushing: last offset delta: {}", last_offset_delta);
            self.end_offset = self.end_offset + last_offset_delta as Offset + 1;
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

/// compute total number of values in the default batch
fn compute_batch_record_size(batch: &Batch) -> usize {
    batch
        .records()
        .iter()
        .fold(0, |acc, batch| acc + batch.value.len())
}

#[cfg(test)]
mod tests {

    use tracing::debug;
    use std::env::temp_dir;
    use std::fs::metadata;
    use std::io::Cursor;
    use std::path::PathBuf;

    use flv_util::fixture::ensure_new_dir;
    use dataplane::batch::{Batch, MemoryRecords};
    use dataplane::Size;
    use dataplane::core::Decoder;
    use dataplane::fixture::create_batch_with_producer;
    use dataplane::fixture::create_batch;
    use dataplane::fixture::read_bytes_from_file;

    use super::MutableSegment;

    use crate::config::ConfigOption;
    use crate::index::OffsetPosition;

    // TODO: consolidate

    fn default_option(base_dir: PathBuf, index_max_interval_bytes: Size) -> ConfigOption {
        ConfigOption {
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

        let option = default_option(test_dir.clone(), 0);

        let base_offset = 20;

        let mut active_segment = MutableSegment::create(base_offset, &option)
            .await
            .expect("create");
        assert_eq!(active_segment.get_end_offset(), 20);

        // batch of 1
        active_segment
            .write_batch(&mut create_batch_with_producer(100, 1))
            .await
            .expect("write");
        assert_eq!(active_segment.get_end_offset(), 21);

        // check to see if batch is written
        let bytes = read_bytes_from_file(&test_dir.join(TEST_FILE_NAME)).expect("read bytes");
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
        assert_eq!(offset_position.get_base_offset(), 20);
        assert_eq!(offset_position.get_pos(), 0); //
        assert_eq!(offset_position.len(), 58);
        assert!((active_segment.find_offset_position(30).await.expect("find")).is_none());
    }

    #[fluvio_future::test]
    async fn test_segment_multiple_record() {
        let test_dir = temp_dir().join("seg-multiple-record");
        ensure_new_dir(&test_dir).expect("new");

        let option = default_option(test_dir.clone(), 0);

        let base_offset = 20;

        let mut active_segment = MutableSegment::create(base_offset, &option)
            .await
            .expect("segment");

        active_segment
            .write_batch(&mut create_batch_with_producer(100, 4))
            .await
            .expect("batch");

        // each record contains 9 bytes

        // check to see if batch is written
        let bytes = read_bytes_from_file(&test_dir.join(TEST_FILE_NAME)).expect("read");
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
        assert_eq!(offset_position.get_base_offset(), 20);
        assert_eq!(offset_position.get_pos(), 0); //
        assert_eq!(offset_position.len(), 85);
        assert!((active_segment.find_offset_position(30).await.expect("find")).is_none());
    }

    const TEST2_FILE_NAME: &str = "00000000000000000040.log"; // offset 20 different from other test

    #[fluvio_future::test]
    async fn test_segment_multiple_batch() {
        let test_dir = temp_dir().join("multiple-segment");
        ensure_new_dir(&test_dir).expect("new");

        let base_offset = 40;

        let option = default_option(test_dir.clone(), 50);

        let mut seg_sink = MutableSegment::create(base_offset, &option)
            .await
            .expect("write");
        seg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("write");
        seg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("write");
        seg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("write");

        assert_eq!(seg_sink.get_end_offset(), 46);

        assert_eq!(seg_sink.get_log_pos(), 237); // each takes 79 bytes

        let index = seg_sink.get_index();
        assert_eq!(index[0].to_be(), (2, 79));

        let bytes = read_bytes_from_file(&test_dir.join(TEST2_FILE_NAME)).expect("read");
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
        assert_eq!(offset_pos1.get_base_offset(), 40);
        assert_eq!(offset_pos1.get_pos(), 0);
        assert_eq!(offset_pos1.len(), 67);
        let offset_pos2 = seg_sink
            .find_offset_position(42)
            .await
            .expect("pos")
            .unwrap();
        assert_eq!(offset_pos2.get_base_offset(), 42);
        assert_eq!(offset_pos2.get_pos(), 79);
        assert_eq!(offset_pos2.len(), 67);

        let offset_pos3 = seg_sink
            .find_offset_position(44)
            .await
            .expect("pos")
            .unwrap();
        assert_eq!(offset_pos3.get_base_offset(), 44);
        assert_eq!(offset_pos3.get_pos(), 158);
        assert_eq!(offset_pos3.len(), 67);

        // test whether you can send batch with non zero base offset
        let mut next_batch = create_batch();
        next_batch.base_offset = 46;
        assert!(seg_sink.write_batch(&mut next_batch).await.is_ok());

        let mut fail_batch = create_batch();
        fail_batch.base_offset = 45;
        assert!(seg_sink.write_batch(&mut fail_batch).await.is_err());
    }
}
