use std::cmp::min;
use std::fs::OpenOptions;
use std::io::Error as IoError;
use std::io::Cursor;
use std::io::ErrorKind;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;

use tracing::trace;
use tracing::debug;
use memmap::Mmap;
use async_trait::async_trait;

use fluvio_future::task::spawn_blocking;

use dataplane::batch::{
    Batch, BatchRecords, BATCH_PREAMBLE_SIZE, BATCH_HEADER_SIZE, BATCH_FILE_HEADER_SIZE,
    MemoryRecords,
};
use dataplane::Size;

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
    pub(crate) fn read_from<S: StorageBytesIterator>(
        file: &mut S,
    ) -> Result<Option<FileBatchPos<R>>, IoError> {
        let pos = file.get_pos();
        let (bytes, read_len) = file.read_bytes(BATCH_FILE_HEADER_SIZE as u32);
        trace!(
            read_len,
            bytes_len = bytes.len(),
            BATCH_FILE_HEADER_SIZE,
            "file batch: read preamble and header",
        );

        if read_len == 0 {
            trace!("no more bytes,there are no more batches");
            return Ok(None);
        }

        if read_len < BATCH_FILE_HEADER_SIZE as u32 {
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

        let mut batch_position = FileBatchPos::new(batch, pos);

        let remainder = batch_position.len() as usize - BATCH_HEADER_SIZE as usize;
        trace!(
            last_offset = batch_position.get_batch().get_last_offset_delta(),
            remainder,
            pos,
            "decoding header",
        );

        batch_position.read_records(file, remainder)?;

        Ok(Some(batch_position))
    }

    /// decode the records of contents
    fn read_records<'a, S>(&'a mut self, file: &'a mut S, content_len: usize) -> Result<(), IoError>
    where
        S: StorageBytesIterator,
    {
        // for now se
        let (bytes, read_len) = file.read_bytes(content_len as u32);

        trace!(
            "file batch: read records {} bytes out of {}",
            read_len,
            content_len
        );

        if read_len < content_len as u32 {
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

// stream to iterate batch
pub struct FileBatchStream<R = MemoryRecords, S = SequentialMmap> {
    invalid: Option<IoError>,
    byte_iterator: S,
    data: PhantomData<R>,
}

impl<R, S> FileBatchStream<R, S>
where
    R: Default + Debug,
    S: StorageBytesIterator,
{
    pub async fn open<P>(path: P) -> Result<FileBatchStream<R, S>, IoError>
    where
        P: AsRef<Path> + Send,
    {
        let byte_iterator = S::open(path).await?;

        //trace!("opening batch stream on: {}",file);
        Ok(Self {
            byte_iterator,
            invalid: None,
            data: PhantomData,
        })
    }

    /// check if it is invalid
    pub fn invalid(self) -> Option<IoError> {
        self.invalid
    }

    #[inline(always)]
    pub fn get_pos(&self) -> Size {
        self.byte_iterator.get_pos()
    }

    #[inline(always)]
    pub fn seek(&mut self, pos: Size) {
        self.byte_iterator.seek(pos);
    }

    #[inline(always)]
    pub fn set_absolute(&mut self, abs_offset: Size) {
        self.byte_iterator.set_absolute(abs_offset);
    }
}

impl<R, S> FileBatchStream<R, S>
where
    R: BatchRecords,
    S: StorageBytesIterator,
{
    pub async fn next(&mut self) -> Option<FileBatchPos<R>> {
        trace!(pos = self.get_pos(), "reading next from");
        match FileBatchPos::read_from(&mut self.byte_iterator) {
            Ok(batch_res) => batch_res,
            Err(err) => {
                debug!("error getting batch: {}", err);
                self.invalid = Some(err);
                None
            }
        }
    }
}

pub struct SequentialMmap {
    map: Mmap,
    pos: Size,
}

#[async_trait]
impl StorageBytesIterator for SequentialMmap {
    async fn open<P: AsRef<Path> + Send>(path: P) -> Result<Self, IoError> {
        let m_path = path.as_ref().to_owned();
        let (mmap, _file, _) = spawn_blocking(move || {
            let mfile = OpenOptions::new().read(true).open(&m_path).unwrap();
            let meta = mfile.metadata().unwrap();
            if meta.len() == 0 {
                // if file size is zero, we can't map it, and there is no offset, se return error
                return Err(IoError::new(ErrorKind::UnexpectedEof, "file size is zero"));
            }

            unsafe { Mmap::map(&mfile) }.map(|mm_file| (mm_file, mfile, m_path))
        })
        .await?;

        Ok(Self { map: mmap, pos: 0 })
    }

    fn read_bytes(&mut self, len: Size) -> (&[u8], Size) {
        // println!("inner len: {}, read_len: {}", self.map.len(),len);
        let bytes = (&self.map).split_at(self.pos as usize).1;
        let prev_pos = self.pos;
        self.pos = min(self.map.len() as Size, self.pos as Size + len);
        // println!("prev pos: {}, new pos: {}", prev_pos, self.pos);
        (bytes, self.pos - prev_pos)
    }

    // seek relative
    fn seek(&mut self, amount: Size) -> Size {
        self.pos = min(self.map.len() as Size, self.pos as Size + amount);
        self.pos
    }

    // seek absolute
    fn set_absolute(&mut self, offset: Size) -> Size {
        self.pos = min(self.map.len() as Size, offset);
        self.pos
    }

    #[inline(always)]
    fn get_pos(&self) -> Size {
        self.pos
    }
}

/// Iterate over some kind of storage with bytes
#[async_trait]
pub trait StorageBytesIterator: Sized {
    async fn open<P: AsRef<Path> + Send>(path: P) -> Result<Self, IoError>;

    fn get_pos(&self) -> Size;

    /// return slice of bytes at current position
    fn read_bytes(&mut self, len: Size) -> (&[u8], Size);

    /// seek relative
    fn seek(&mut self, amount: Size) -> Size;

    /// set absolute position
    fn set_absolute(&mut self, offset: Size) -> Size;
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::path::PathBuf;

    use flv_util::fixture::ensure_new_dir;
    use dataplane::fixture::create_batch;
    use dataplane::fixture::create_batch_with_producer;

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
            .write_batch(&mut create_batch())
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
            .write_batch(&mut create_batch())
            .await
            .expect("write");
        active_segment
            .write_batch(&mut create_batch_with_producer(25, 2))
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
