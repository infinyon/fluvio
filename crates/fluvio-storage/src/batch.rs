use std::cmp::min;
use std::io::Error as IoError;
use std::io::Cursor;
use std::io::ErrorKind;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;

use fluvio_future::fs::mmap::MemoryMappedFile;
use tracing::trace;
use tracing::debug;

use fluvio_future::fs::File;
use dataplane::batch::{
    Batch, BatchRecords, BATCH_PREAMBLE_SIZE, BATCH_HEADER_SIZE, BATCH_FILE_HEADER_SIZE,
    MemoryRecords,
};
use dataplane::Size;
use dataplane::Offset;

use crate::StorageError;

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

    pub fn get_batch(&self) -> &Batch<R> {
        &self.inner
    }

    pub fn get_pos(&self) -> Size {
        self.pos
    }

    pub fn get_base_offset(&self) -> Offset {
        self.inner.get_base_offset()
    }

    pub fn get_last_offset(&self) -> Offset {
        self.inner.get_last_offset()
    }

    /// batch length (without preamble)
    pub fn len(&self) -> Size {
        self.inner.batch_len as Size
    }

    /// total batch length including preamble
    pub fn total_len(&self) -> Size {
        self.len() + BATCH_PREAMBLE_SIZE as Size
    }

    pub fn records_remainder_bytes(&self, remainder: usize) -> usize {
        self.inner.records().remainder_bytes(remainder)
    }

    /// decode next batch from file
    pub(crate) async fn from(
        file: &mut SequentialMmap,
        pos: Size,
    ) -> Result<Option<FileBatchPos<R>>, IoError> {
        let (bytes, read_len) = file.read_bytes(BATCH_FILE_HEADER_SIZE as u32);
        trace!(
            "file batch: read preamble and header {} bytes out of {}",
            read_len,
            BATCH_FILE_HEADER_SIZE
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
        let mut batch = Batch::default();
        batch.decode_from_file_buf(&mut cursor, 0)?;
        let mut file_batch = FileBatchPos::new(batch, pos);

        let remainder = file_batch.len() as usize - BATCH_HEADER_SIZE as usize;
        trace!(
            "file batch: offset: {}, len: {}, total: {}, remainder: {}, pos: {}",
            file_batch.get_batch().get_last_offset_delta(),
            file_batch.len(),
            file_batch.total_len(),
            remainder,
            pos
        );

        if file_batch.records_remainder_bytes(remainder) > 0 {
            trace!("file batch reading records with remainder: {}", remainder);
            file_batch.read_records(file, remainder).await?
        } else {
            trace!("file batch seeking next batch");
            file_batch.seek_to_next_batch(file, remainder).await?;
        }

        Ok(Some(file_batch))
    }

    /// decode the records
    async fn read_records<'a>(
        &'a mut self,
        file: &'a mut SequentialMmap,
        buf_len: usize,
    ) -> Result<(), IoError> {
        let (bytes, read_len) = file.read_bytes(buf_len as u32);

        trace!(
            "file batch: read records {} bytes out of {}",
            read_len,
            buf_len
        );

        if read_len < buf_len as u32 {
            return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                "not enough for records",
            ));
        }

        let mut cursor = Cursor::new(bytes);
        self.inner.mut_records().decode(&mut cursor, 0)?;

        Ok(())
    }

    async fn seek_to_next_batch<'a>(
        &'a self,
        file: &'a mut SequentialMmap,
        remainder: usize,
    ) -> Result<(), IoError> {
        if remainder > 0 {
            trace!("file batch skipping: content {} bytes", remainder);
            let seek_position = file.seek(remainder as u32);
            trace!("file batch new position: {}", seek_position);
        }

        Ok(())
    }
}

struct SequentialMmap {
    map: MemoryMappedFile,
    pos: Size,
}

impl SequentialMmap {
    // read bytes
    fn read_bytes(&mut self, len: Size) -> (&[u8], Size) {
        let inner = self.map.inner();
        let prev_pos = self.pos;
        self.pos = min(inner.len() as Size, self.pos as Size + len);
        (&inner, self.pos - prev_pos)
    }

    fn seek(&mut self, amount: Size) -> Size {
        let inner = self.map.inner();
        self.pos = min(inner.len() as Size, self.pos as Size + amount);
        self.pos
    }
}

// stream to iterate batch
pub struct FileBatchStream<R = MemoryRecords> {
    pos: Size,
    invalid: Option<IoError>,
    file: File,
    seq_map: SequentialMmap,
    data: PhantomData<R>,
}

impl<R> FileBatchStream<R>
where
    R: Default + Debug,
{
    pub async fn open<P>(path: P) -> Result<FileBatchStream<R>, IoError>
    where
        P: AsRef<Path>,
    {
        let (mmap, file) = MemoryMappedFile::open(path, 0 as u64).await?;

        let seq_map = SequentialMmap {
            map: mmap,
            pos: 0 as Size,
        };
        //trace!("opening batch stream on: {}",file);
        Ok(Self {
            pos: 0,
            file,
            seq_map,
            invalid: None,
            data: PhantomData,
        })
    }

    pub async fn new_with_pos(
        mut file: File,
        pos: Size,
    ) -> Result<FileBatchStream<R>, StorageError> {
        /*
        trace!("opening batch  stream at: {}", pos);
        let seek_position = file.seek(SeekFrom::Start(pos as u64)).await?;
        if seek_position != pos as u64 {
            return Err(IoError::new(ErrorKind::UnexpectedEof, "not enough for position").into());
        }
        Ok(FileBatchStream {
            pos,
            file,
            invalid: None,
            data: PhantomData,
        })
        */
        todo!()
    }

    /// check if it is invalid
    pub fn invalid(self) -> Option<IoError> {
        self.invalid
    }
}

impl<R> FileBatchStream<R>
where
    R: BatchRecords,
{
    pub async fn next(&mut self) -> Option<FileBatchPos<R>> {
        trace!("reading next from pos: {}", self.pos);
        match FileBatchPos::from(&mut self.seq_map, self.pos).await {
            Ok(batch_res) => {
                if let Some(ref batch) = batch_res {
                    trace!("batch founded, updating pos");
                    self.pos += batch.total_len() as Size;
                } else {
                    trace!("no batch founded");
                }
                batch_res
            }
            Err(err) => {
                debug!("error getting batch: {}", err);
                self.invalid = Some(err);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::path::PathBuf;

    use flv_util::fixture::ensure_new_dir;
    use dataplane::fixture::create_batch;
    use dataplane::fixture::create_batch_with_producer;

    use crate::config::ConfigOption;
    use crate::segment::MutableSegment;

    fn default_option(base_dir: PathBuf) -> ConfigOption {
        ConfigOption {
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

        let option = default_option(test_dir.clone());

        let mut active_segment = MutableSegment::create(300, &option).await.expect("segment");

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
        assert_eq!(batch1.get_last_offset(), 301);
    }

    #[fluvio_future::test]
    async fn test_batch_stream_multiple() {
        let test_dir = temp_dir().join("batch-stream");
        ensure_new_dir(&test_dir).expect("new");

        let option = default_option(test_dir.clone());

        let mut active_segment = MutableSegment::create(300, &option).await.expect("create");

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

        let batch1 = batch_stream.next().await.expect("batch");
        assert_eq!(batch1.get_last_offset(), 301);
        let batch2 = batch_stream.next().await.expect("batch");
        assert_eq!(batch2.get_last_offset(), 303);
    }
}
