use std::io::Error as IoError;
use std::io::Cursor;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;

use fluvio_protocol::record::BatchHeader;
use fluvio_protocol::record::Offset;
use tracing::error;
use tracing::instrument;
use tracing::trace;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;

use fluvio_future::fs::File;
use fluvio_protocol::record::{
    Batch, BatchRecords, BATCH_HEADER_SIZE, BATCH_FILE_HEADER_SIZE, MemoryRecords,
};
use fluvio_protocol::record::Size;

use crate::file::FileBytesIterator;

#[derive(Debug, thiserror::Error, Clone)]
/// Outer batch representation
/// It's either sucessfully decoded into actual batch or not enough bytes to decode
pub enum BatchHeaderError {
    #[error("Not Enough Header {pos}, {actual_len} {expected_len}")]
    NotEnoughHeader {
        pos: u32,
        actual_len: usize,
        expected_len: usize,
    },
    #[error("Not Enough Content {pos} {base_offset} {actual_len} {expected_len}")]
    NotEnoughContent {
        header: BatchHeader,
        base_offset: Offset,
        pos: u32,
        actual_len: usize,
        expected_len: usize,
    },
}

/// hold information about position of batch in the file
pub struct FileBatchPos<R>
where
    R: BatchRecords,
{
    inner: Batch<R>,
    pos: Size,
}

impl<R> Unpin for FileBatchPos<R> where R: BatchRecords {}

impl<R> FileBatchPos<R>
where
    R: BatchRecords,
{
    /// read from storage byt iterator
    #[instrument(skip(file))]
    pub(crate) async fn read_from<S: StorageBytesIterator>(
        file: &mut S,
    ) -> Result<Option<FileBatchPos<R>>> {
        let pos = file.get_pos();
        trace!(pos, "reading from pos");
        let bytes = match file.read_bytes(BATCH_FILE_HEADER_SIZE as u32).await? {
            Some(bytes) => bytes,

            None => {
                trace!("no bytes read, returning empty");
                return Ok(None);
            }
        };

        let read_len = bytes.len();
        trace!(
            read_len,
            bytes_len = bytes.len(),
            BATCH_FILE_HEADER_SIZE,
            "readed batch header",
        );

        if read_len < BATCH_FILE_HEADER_SIZE {
            return Err(BatchHeaderError::NotEnoughHeader {
                actual_len: read_len,
                expected_len: BATCH_FILE_HEADER_SIZE,
                pos,
            }
            .into());
        }

        let mut cursor = Cursor::new(bytes);
        let mut batch: Batch<R> = Batch::default();
        batch.decode_from_file_buf(&mut cursor, 0)?;
        trace!("batch: {:#?}", batch);
        /*
        if !batch.validate_decoding() {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!("{:#?}", batch),
            ));
        }
        */

        let content_len = batch.batch_len as usize - BATCH_HEADER_SIZE;
        trace!(
            last_offset = batch.get_last_offset(),
            content_len,
            pos,
            "trying to read batch content",
        );

        // for now se
        let bytes = match file.read_bytes(content_len as u32).await? {
            Some(bytes) => bytes,
            None => {
                return Err(BatchHeaderError::NotEnoughContent {
                    base_offset: batch.get_base_offset(),
                    header: batch.header,
                    actual_len: 0,
                    expected_len: content_len,
                    pos,
                }
                .into())
            }
        };

        // get actual bytes read
        let read_len = bytes.len();

        trace!(
            "file batch: read records {} bytes out of {}",
            read_len,
            content_len
        );

        if read_len < content_len {
            return Err(BatchHeaderError::NotEnoughContent {
                base_offset: batch.get_base_offset(),
                header: batch.header,
                actual_len: read_len,
                expected_len: content_len,
                pos,
            }
            .into());
        }

        let mut cursor = Cursor::new(bytes);
        batch.mut_records().decode(&mut cursor, 0)?;

        Ok(Some(FileBatchPos { inner: batch, pos }))
    }

    #[inline(always)]
    pub fn get_pos(&self) -> Size {
        self.pos
    }

    #[inline(always)]
    pub fn get_batch(&self) -> &Batch<R> {
        &self.inner
    }

    pub fn inner(self) -> Batch<R> {
        self.inner
    }
}

// Stream to iterate over batches in a file
pub struct FileBatchStream<R = MemoryRecords, S = FileBytesIterator> {
    invalid: bool,
    byte_iterator: S,
    data: PhantomData<R>,
}

impl<R, S> FileBatchStream<R, S>
where
    R: Default + Debug,
    S: StorageBytesIterator,
{
    // mark as invalid
    pub fn is_invalid(&self) -> bool {
        self.invalid
    }

    #[inline(always)]
    pub fn get_pos(&self) -> Size {
        self.byte_iterator.get_pos()
    }

    pub async fn seek(&mut self, pos: Size) -> Result<(), IoError> {
        self.byte_iterator.seek(pos).await?;
        Ok(())
    }

    pub async fn set_absolute(&mut self, abs_offset: Size) -> Result<(), IoError> {
        self.byte_iterator.set_absolute(abs_offset).await?;
        Ok(())
    }
}

impl<R, S> FileBatchStream<R, S>
where
    R: BatchRecords,
    S: StorageBytesIterator,
{
    pub async fn open<P>(path: P) -> Result<FileBatchStream<R, S>, IoError>
    where
        P: AsRef<Path> + Send,
    {
        let byte_iterator = S::open(path).await?;

        Ok(Self {
            byte_iterator,
            invalid: false,
            data: PhantomData,
        })
    }

    pub async fn from_file(file: File) -> Result<FileBatchStream<R, S>, IoError> {
        let byte_iterator = S::from_file(file).await?;

        Ok(Self {
            byte_iterator,
            invalid: false,
            data: PhantomData,
        })
    }

    /// get next batch position
    /// if there is an error, it will be stored in `invalid` field
    #[instrument(skip(self))]
    pub async fn try_next(&mut self) -> Result<Option<FileBatchPos<R>>> {
        if self.invalid {
            return Err(anyhow!("stream has been invalidated"));
        }
        match FileBatchPos::read_from(&mut self.byte_iterator).await {
            Ok(batch_res) => Ok(batch_res),
            Err(err) => {
                error!("error getting batch: {}, invalidating", err);
                self.invalid = true;
                Err(err)
            }
        }
    }
}

/// Iterate over some kind of storage with bytes
#[async_trait]
pub trait StorageBytesIterator: Sized {
    async fn from_file(file: File) -> Result<Self, IoError>;

    async fn open<P: AsRef<Path> + Send>(path: P) -> Result<Self, IoError>;

    fn get_pos(&self) -> Size;

    /// return slice of bytes at current position
    async fn read_bytes(&mut self, len: Size) -> Result<Option<Bytes>, IoError>;

    /// seek relative
    async fn seek(&mut self, amount: Size) -> Result<Size, IoError>;

    /// set absolute position
    async fn set_absolute(&mut self, offset: Size) -> Result<Size, IoError>;
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::path::PathBuf;

    use flv_util::fixture::ensure_new_dir;
    use fluvio_protocol::fixture::create_batch;
    use fluvio_protocol::fixture::create_batch_with_producer;

    use crate::config::ReplicaConfig;
    use crate::segment::MutableSegment;

    fn default_option(base_dir: PathBuf) -> ReplicaConfig {
        ReplicaConfig {
            base_dir,
            segment_max_bytes: 1000,
            index_max_bytes: 1000,
            ..Default::default()
        }
    }

    #[fluvio_future::test]
    async fn test_batch_stream_single() {
        let test_dir = temp_dir().join("batch-stream-single");
        ensure_new_dir(&test_dir).expect("new");

        let option = default_option(test_dir.clone()).shared();

        let mut active_segment = MutableSegment::create(300, option).await.expect("segment");

        active_segment
            .append_batch(&mut create_batch())
            .await
            .expect("writing batches");

        let mut batch_stream = active_segment
            .open_batch_header_stream(0)
            .await
            .expect("open file batch stream");
        let batch1 = batch_stream.try_next().await.expect("ok").expect("batch");
        let pos = batch1.get_pos();
        let batch = batch1.inner();
        assert_eq!(batch.get_base_offset(), 300);
        assert_eq!(batch.get_header().producer_id, 12);
        assert_eq!(batch.get_last_offset(), 301);
        assert_eq!(pos, 0);
    }

    #[fluvio_future::test]
    async fn test_batch_stream_multiple() {
        let test_dir = temp_dir().join("batch-stream-multiple");
        ensure_new_dir(&test_dir).expect("new");

        let option = default_option(test_dir.clone()).shared();

        let mut active_segment = MutableSegment::create(300, option).await.expect("create");

        active_segment
            .append_batch(&mut create_batch())
            .await
            .expect("write");
        active_segment
            .append_batch(&mut create_batch_with_producer(25, 2))
            .await
            .expect("batch");

        let mut batch_stream = active_segment
            .open_batch_header_stream(0)
            .await
            .expect("open file batch stream");

        assert_eq!(batch_stream.get_pos(), 0);
        let batch1 = batch_stream.try_next().await.expect("ok").expect("batch");
        assert_eq!(batch1.get_batch().get_last_offset(), 301);
        assert_eq!(batch_stream.get_pos(), 79);
        let batch2 = batch_stream.try_next().await.expect("ok").expect("batch");
        assert_eq!(batch2.get_batch().get_last_offset(), 303);
    }
}
