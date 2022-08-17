use std::io::Error as IoError;
use std::io::Cursor;
use std::io::ErrorKind;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;

use fluvio_future::fs::File;
use tracing::instrument;
use tracing::trace;
use tracing::debug;
use async_trait::async_trait;
use bytes::Bytes;

use fluvio_protocol::record::{
    Batch, BatchRecords, BATCH_PREAMBLE_SIZE, BATCH_HEADER_SIZE, BATCH_FILE_HEADER_SIZE,
    MemoryRecords,
};
use fluvio_protocol::record::Size;

use crate::file::FileBytesIterator;

/// hold information about position of batch in the file
pub struct FileBatchPos<R>
where
    R: BatchRecords,
{
    inner: Batch<R>,
    pos: Size,
}

impl<R> Unpin for FileBatchPos<R> where R: BatchRecords {}

#[allow(clippy::len_without_is_empty)]
impl<R> FileBatchPos<R>
where
    R: BatchRecords,
{
    fn new(inner: Batch<R>, pos: Size) -> Self {
        FileBatchPos { inner, pos }
    }

    #[inline(always)]
    pub fn get_batch(&self) -> &Batch<R> {
        &self.inner
    }

    #[inline(always)]
    pub fn get_pos(&self) -> Size {
        self.pos
    }

    /// batch length (without preamble)
    #[inline(always)]
    pub fn len(&self) -> Size {
        self.inner.batch_len as Size
    }

    /// total batch length including preamble
    #[inline(always)]
    pub fn total_len(&self) -> Size {
        self.len() + BATCH_PREAMBLE_SIZE as Size
    }

    /// decode next batch from sequence map
    #[instrument(skip(file))]
    pub(crate) async fn read_from<S: StorageBytesIterator>(
        file: &mut S,
    ) -> Result<Option<FileBatchPos<R>>, IoError> {
        let pos = file.get_pos();
        let bytes = match file.read_bytes(BATCH_FILE_HEADER_SIZE as u32).await? {
            Some(bytes) => bytes,

            None => {
                trace!("no more bytes,there are no more batches");
                return Ok(None);
            }
        };

        let read_len = bytes.len();
        trace!(
            read_len,
            bytes_len = bytes.len(),
            BATCH_FILE_HEADER_SIZE,
            "file batch: read preamble and header",
        );

        if read_len < BATCH_FILE_HEADER_SIZE {
            return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "expected: {} but only {} bytes read",
                    BATCH_FILE_HEADER_SIZE, read_len
                ),
            ));
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

        let mut batch_position = FileBatchPos::new(batch, pos);

        let remainder = batch_position.len() as usize - BATCH_HEADER_SIZE as usize;
        trace!(
            last_offset = batch_position.get_batch().get_last_offset(),
            remainder,
            pos,
            "decoding header",
        );

        batch_position.read_records(file, remainder).await?;

        Ok(Some(batch_position))
    }

    /// decode the records of contents
    async fn read_records<'a, S>(
        &'a mut self,
        file: &'a mut S,
        content_len: usize,
    ) -> Result<(), IoError>
    where
        S: StorageBytesIterator,
    {
        // for now se
        let bytes = match file.read_bytes(content_len as u32).await? {
            Some(bytes) => bytes,
            None => {
                return Err(IoError::new(
                    ErrorKind::UnexpectedEof,
                    "not enough for records",
                ));
            }
        };

        let read_len = bytes.len();

        trace!(
            "file batch: read records {} bytes out of {}",
            read_len,
            content_len
        );

        if read_len < content_len {
            return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                "not enough for records",
            ));
        }

        let mut cursor = Cursor::new(bytes);
        self.inner.mut_records().decode(&mut cursor, 0)?;

        Ok(())
    }
}

// Stream to iterate over batches in a file
pub struct FileBatchStream<R = MemoryRecords, S = FileBytesIterator> {
    invalid: Option<IoError>,
    byte_iterator: S,
    data: PhantomData<R>,
}

impl<R, S> FileBatchStream<R, S>
where
    R: Default + Debug,
    S: StorageBytesIterator,
{
    /// check if it is invalid
    pub fn invalid(self) -> Option<IoError> {
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
            invalid: None,
            data: PhantomData,
        })
    }

    pub async fn from_file(file: File) -> Result<FileBatchStream<R, S>, IoError> {
        let byte_iterator = S::from_file(file).await?;

        Ok(Self {
            byte_iterator,
            invalid: None,
            data: PhantomData,
        })
    }

    #[instrument(skip(self))]
    pub async fn next(&mut self) -> Option<FileBatchPos<R>> {
        trace!(pos = self.get_pos(), "reading next from");
        match FileBatchPos::read_from(&mut self.byte_iterator).await {
            Ok(batch_res) => batch_res,
            Err(err) => {
                debug!("error getting batch: {}", err);
                self.invalid = Some(err);
                None
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
        let batch1 = batch_stream.next().await.expect("batch");
        let batch = batch1.get_batch();
        assert_eq!(batch.get_base_offset(), 300);
        assert_eq!(batch.get_header().producer_id, 12);
        assert_eq!(batch1.get_batch().get_last_offset(), 301);
        assert_eq!(batch1.get_pos(), 0);
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
        let batch1 = batch_stream.next().await.expect("batch");
        assert_eq!(batch1.get_batch().get_last_offset(), 301);
        assert_eq!(batch_stream.get_pos(), 79);
        let batch2 = batch_stream.next().await.expect("batch");
        assert_eq!(batch2.get_batch().get_last_offset(), 303);
    }
}
