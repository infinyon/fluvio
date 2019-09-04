use std::fmt;
use std::io::Error as IoError;
use std::pin::Pin;
use std::ops::Deref;
use std::task::Context;
use std::task::Poll;

use futures::sink::Sink;
use futures::stream::StreamExt;
use futures::ready;
use futures::Future;
use log::debug;
use log::trace;
use pin_utils::pin_mut;
use pin_utils::unsafe_pinned;
use pin_utils::unsafe_unpinned;

use kf_protocol::api::DefaultBatch;
use kf_protocol::api::Offset;
use kf_protocol::api::Size;
use future_aio::fs::AsyncFileSlice;

use crate::BatchHeaderStream;
use crate::mut_index::MutLogIndex;
use crate::index::LogIndex;
use crate::index::Index;
use crate::records::FileRecords;
use crate::mut_records::MutFileRecords;
use crate::records::FileRecordsSlice;
use crate::BatchHeaderPos;
use crate::ConfigOption;
use crate::StorageError;
use crate::DefaultFileBatchStream;
use crate::index::OffsetPosition;
use crate::validator::LogValidationError;
use crate::util::OffsetError;

pub(crate) type MutableSegment = Segment<MutLogIndex,MutFileRecords>;
pub(crate) type ReadSegment = Segment<LogIndex,FileRecordsSlice>;


pub(crate) enum SegmentSlice<'a> {
    MutableSegment(&'a MutableSegment),
    Segment(&'a ReadSegment)
}


impl <'a>Unpin for SegmentSlice<'a> {}

impl <'a>SegmentSlice<'a> {
    pub fn new_mut_segment(segment: &'a MutableSegment) -> Self {
        SegmentSlice::MutableSegment(segment)
    }

    pub fn new_segment(segment: &'a ReadSegment) -> Self {
        SegmentSlice::Segment(segment)
    }

    
}





/// Segment contains both message log and index
pub(crate) struct Segment<I,L> {
    option: ConfigOption,
    msg_log: L,
    index: I,
    base_offset: Offset,
    end_offset: Offset,
}

impl <I,L>fmt::Debug for Segment<I,L> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Segment {{base: {}, current: {} }}",
            self.get_base_offset(), self.get_end_offset()
        )
    }
}

impl <I,L> Segment<I,L> {

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


impl <I,L> Segment<I,L> 
    where I:Index,
        I: Deref<Target=[(Size,Size)]>,
        L: FileRecords

 {

    #[allow(dead_code)]
    pub fn get_index(&self) -> &I {
        &self.index
    }

    pub async fn open_batch_header_stream(
        &self,
        start_pos: Size,
    ) -> Result<BatchHeaderStream, StorageError> {
        trace!("opening batch headere stream at: {}", start_pos);
        let file = self.msg_log.get_file().read_clone().await?;
        BatchHeaderStream::new_with_pos(file, start_pos).await
    }

    
    #[allow(dead_code)]
    pub async fn open_default_batch_stream(&self) -> Result<DefaultFileBatchStream,StorageError> {
        let file = self.msg_log.get_file().read_clone().await?;
        Ok(DefaultFileBatchStream::new(file))
    }


    /// get file slice from offset to end of segment
    pub async fn records_slice(&self,start_offset: Offset,max_offset_opt: Option<Offset>) -> Result<Option<AsyncFileSlice>,StorageError> {

        match self.find_offset_position(start_offset).await? {
            Some(start_pos) => {
                trace!("found batch: {:#?} at: {}",start_pos.get_batch(),start_pos.get_pos());
                match max_offset_opt {
                    Some(max_offset) =>  {
                        // check if max offset same as segment endset
                        if max_offset == self.get_end_offset() {
                            trace!("max offset is same as end offset, reading to end");
                            Ok(Some(self.msg_log.as_file_slice(start_pos.get_pos())?))
                        } else {
                            trace!("end offset is supplied: {}",max_offset);
                            match self.find_offset_position(max_offset).await? {
                                Some(end_pos) => {
                                    Ok(Some(self.msg_log.as_file_slice_from_to(start_pos.get_pos(),end_pos.get_pos() - start_pos.get_pos())?))
                                },
                                None => Err(StorageError::OffsetError(OffsetError::NotExistent))
                            }
                        }
                    },                        
                    None => Ok(Some(self.msg_log.as_file_slice(start_pos.get_pos())?))
                }
                
            }
            None => Ok(None)
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
        trace!(
            "found relative pos: {}",
            position
        );

        let mut header_stream = self.open_batch_header_stream(position).await?;
        trace!("iterating header stream");
        while let Some(batch_pos) = header_stream.next().await {
            let last_offset = batch_pos.get_last_offset();
            if last_offset >= offset {
                trace!(
                    "found batch last offset which matches offset: {}",
                    last_offset
                );
                return Ok(Some(batch_pos));
            } else {
                trace!(
                    "skipping batch end offset: {}",
                    last_offset
                );
            }
        }
        Ok(None)
    }
}

impl Segment<LogIndex,FileRecordsSlice> {

    pub async fn open_for_read(base_offset: Offset, option: &ConfigOption) -> Result<Self, StorageError> {
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


impl Segment<MutLogIndex,MutFileRecords> {
    unsafe_pinned!(msg_log: MutFileRecords);
    unsafe_pinned!(index: MutLogIndex);
    unsafe_unpinned!(base_offset: Offset);
    unsafe_unpinned!(end_offset: Offset);

    // create segment on base directory
    pub async fn create(
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<MutableSegment, StorageError> {
        debug!("creating new segment: offset: {}", base_offset);
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
            base_offset, &option.base_dir
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

    async fn shrink_index(&mut self) -> Result<(),IoError> {
        self.index.shrink().await
    }

    /*
    // do modification under 'a lifetime so we do multiple mutations
    #[allow(dead_code)]
    fn update_send(self, mut batch: DefaultRecordBatch) -> Result<(), StorageError> {
        batch.set_base_offset(self.current_offset);
        Ok(())
    }
    */

    // use poll rather than future to make it easier to call from other poll
    // otherwise you get complex lifetime issue

    pub fn poll_roll_over(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), IoError>> {
        let ft = self.shrink_index();
        pin_mut!(ft);
        ft.poll(cx)
    }


    /// convert to immutable segment
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

   
}

impl Unpin for Segment<MutLogIndex,MutFileRecords> {}

impl Sink<DefaultBatch> for Segment<MutLogIndex,MutFileRecords> {
   
    type Error = StorageError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.msg_log().poll_ready(cx)
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        mut item: DefaultBatch,
    ) -> Result<(), Self::Error> {

        let current_offset = self.as_ref().end_offset;
        let base_offset = self.as_ref().base_offset;
        let pos = self.as_ref().get_log_pos();

        // fill in the baseoffset using current offset if record's batch offset is 0
        // ensure batch is not already recorded
        if item.base_offset == 0 {
             item.set_base_offset(current_offset);
        } else {
            if item.base_offset < current_offset {
                return Err(StorageError::LogValidationError(LogValidationError::ExistingBatch))
            }
        }
       
        let batch_offset_delta = (current_offset - base_offset) as i32; 
        debug!(
            "writing batch with base: {}, file pos: {}",
            base_offset, pos
        );

        match self.as_mut().msg_log().start_send(item) {
            Ok(_) => {
                let batch_len = self.msg_log.get_pending_batch_len();
                self.index()
                    .start_send((batch_offset_delta as u32, pos, batch_len))
                    .map_err(|err| err.into())
            }
            Err(err) => Err(err),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let last_offset_delta = self.as_ref().msg_log.get_item_last_offset_delta();
        trace!("flushing: last offset delta: {}",last_offset_delta);
        ready!(self.as_mut().msg_log().poll_flush(cx))?;
        let offset_pt = self.end_offset();
        *offset_pt = *offset_pt + last_offset_delta as Offset + 1;
        debug!("flushing, updated end offset: {}", *offset_pt);
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.msg_log().poll_close(cx)
    }
}


#[cfg(test)]
mod tests {

    use futures::sink::SinkExt;
    use log::debug;
    use std::env::temp_dir;
    use std::fs::metadata;
    use std::io::Cursor;
    use std::path::PathBuf;

    use future_helper::test_async;

    use kf_protocol::api::DefaultBatch;
    use kf_protocol::api::Size;
    use kf_protocol::Decoder;

    use super::MutableSegment;
    use crate::fixture::create_batch_with_producer;
    use crate::fixture::create_batch;
    use crate::fixture::ensure_new_dir;
    use crate::fixture::read_bytes_from_file;
    use crate::ConfigOption;
    use crate::StorageError;
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

    #[test_async]
    async fn test_segment_single_record() -> Result<(), StorageError> {
        let test_dir = temp_dir().join("seg-single-record");
        ensure_new_dir(&test_dir)?;

        let option = default_option(test_dir.clone(), 0);

        let base_offset = 20;

        let mut seg_sink = MutableSegment::create(base_offset, &option).await?;

        assert_eq!(seg_sink.get_end_offset(),20);
        seg_sink.send(create_batch_with_producer(100,1)).await?;
        assert_eq!(seg_sink.get_end_offset(),21);

        // check to see if batch is written
        let bytes = read_bytes_from_file(&test_dir.join(TEST_FILE_NAME))?;
        debug!("read {} bytes", bytes.len());

        let batch = DefaultBatch::decode_from(&mut Cursor::new(bytes),0)?;
        assert_eq!(batch.get_base_offset(), 20);
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records.len(), 1);

        
        let seg1_metadata = metadata(test_dir.join(SEG_INDEX))?;
        assert_eq!(seg1_metadata.len(), 1000);

        
        assert!((seg_sink.find_offset_position(10).await?).is_none());
        
        let offset_position = (seg_sink.find_offset_position(20).await?).expect("offset exists");
        
        assert_eq!(offset_position.get_base_offset(), 20);
        assert_eq!(offset_position.get_pos(), 0);//
        assert_eq!(offset_position.len(), 70 - 12); 
        assert!((seg_sink.find_offset_position(30).await?).is_none());
        
        Ok(())
    }


    #[test_async]
    async fn test_segment_multiple_record() -> Result<(), StorageError> {
        let test_dir = temp_dir().join("seg-multiple-record");
        ensure_new_dir(&test_dir)?;

        let option = default_option(test_dir.clone(), 0);

        let base_offset = 20;

        let mut seg_sink = MutableSegment::create(base_offset, &option).await?;

        seg_sink.send(create_batch_with_producer(100,4)).await?;



        // each record contains 9 bytes

        // check to see if batch is written
        let bytes = read_bytes_from_file(&test_dir.join(TEST_FILE_NAME))?;
        debug!("read {} bytes", bytes.len());

        let batch = DefaultBatch::decode_from(&mut Cursor::new(bytes),0)?;
        assert_eq!(batch.get_base_offset(), 20);
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records.len(), 4);

        let seg1_metadata = metadata(test_dir.join(SEG_INDEX))?;
        assert_eq!(seg1_metadata.len(), 1000);

        assert!((seg_sink.find_offset_position(10).await?).is_none());
        let offset_position = (seg_sink.find_offset_position(20).await?).expect("offset exists");
        assert_eq!(offset_position.get_base_offset(), 20);
        assert_eq!(offset_position.get_pos(), 0);//
        assert_eq!(offset_position.len(), 85); 
        assert!((seg_sink.find_offset_position(30).await?).is_none());

        Ok(())
    }


    const TEST2_FILE_NAME: &str = "00000000000000000040.log"; // offset 20 different from other test

    #[test_async]
    async fn test_segment_multiple_batch() -> Result<(), StorageError> {
        let test_dir = temp_dir().join("multiple-segment");
        ensure_new_dir(&test_dir)?;

        let base_offset = 40;

        let option = default_option(test_dir.clone(), 50);

        let mut seg_sink = MutableSegment::create(base_offset, &option).await?;
        seg_sink.send(create_batch()).await?;
        seg_sink.send(create_batch()).await?;
        seg_sink.send(create_batch()).await?;

        assert_eq!(seg_sink.get_end_offset(),46);

        assert_eq!(seg_sink.get_log_pos(), 237); // each takes 79 bytes

        let index = seg_sink.get_index();
        assert_eq!(index[0].to_be(), (2, 79));

        let bytes = read_bytes_from_file(&test_dir.join(TEST2_FILE_NAME))?;
        debug!("read {} bytes", bytes.len());

        let cursor = &mut Cursor::new(bytes);
        let batch = DefaultBatch::decode_from(cursor,0)?;
        assert_eq!(batch.get_base_offset(), 40);
        assert_eq!(batch.get_header().last_offset_delta, 1);

        let batch2 = DefaultBatch::decode_from(cursor,0)?;
        assert_eq!(batch2.get_base_offset(), 42);
        assert_eq!(batch2.get_header().last_offset_delta, 1);

        
        let offset_pos1 = seg_sink.find_offset_position(40).await?.expect("pos");
        assert_eq!(offset_pos1.get_base_offset(), 40);
        assert_eq!(offset_pos1.get_pos(), 0);
        assert_eq!(offset_pos1.len(), 67);
        
        
        let offset_pos2 = seg_sink.find_offset_position(42).await?.expect("pos");
        assert_eq!(offset_pos2.get_base_offset(), 42);
        assert_eq!(offset_pos2.get_pos(), 79);
        assert_eq!(offset_pos2.len(), 67);

        
        let offset_pos3 = seg_sink.find_offset_position(44).await?.expect("pos");
        assert_eq!(offset_pos3.get_base_offset(), 44);
        assert_eq!(offset_pos3.get_pos(), 158);
        assert_eq!(offset_pos3.len(), 67);
        

        // test whether you can send batch with non zero base offset
        let mut next_batch = create_batch();
        next_batch.base_offset = 46;
        assert!(seg_sink.send(next_batch).await.is_ok());

        let mut fail_batch = create_batch();
        fail_batch.base_offset = 45;
        assert!(seg_sink.send(fail_batch).await.is_err());


        Ok(())
    }

}
