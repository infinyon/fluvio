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
    pub(crate) fn read_from(file: &mut SequentialMmap) -> Result<Option<FileBatchPos<R>>, IoError> {
        let pos = file.pos;
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
    fn read_records<'a>(
        &'a mut self,
        file: &'a mut SequentialMmap,
        content_len: usize,
    ) -> Result<(), IoError> {
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

        // skip decoding the records

        let mut cursor = Cursor::new(bytes);
        self.inner.mut_records().decode(&mut cursor, 0)?;

        Ok(())
    }
}

/*
impl FileBatchPos<MemoryRecords> {

    /// decode the records of contents
    async fn read_records<'a>(
        &'a mut self,
        file: &'a mut SequentialMmap,
        content_len: usize,
    ) -> Result<(), IoError> {
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
*/

pub struct SequentialMmap {
    map: Mmap,
    pos: Size,
}

impl SequentialMmap {
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
    pub fn set_absolute(&mut self, offset: Size) -> Size {
        self.pos = min(self.map.len() as Size, offset);
        self.pos
    }
}

// stream to iterate batch
pub struct FileBatchStream<R = MemoryRecords> {
    invalid: Option<IoError>,
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

        let seq_map = SequentialMmap { map: mmap, pos: 0 };
        //trace!("opening batch stream on: {}",file);
        Ok(Self {
            seq_map,
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
        self.seq_map.pos
    }

    #[inline(always)]
    pub fn seek(&mut self, pos: Size) {
        self.seq_map.seek(pos);
    }

    #[inline(always)]
    pub fn set_absolute(&mut self, abs_offset: Size) {
        self.seq_map.set_absolute(abs_offset);
    }
}

impl<R> FileBatchStream<R>
where
    R: BatchRecords,
{
    pub async fn next(&mut self) -> Option<FileBatchPos<R>> {
        trace!(pos = self.get_pos(), "reading next from");
        match FileBatchPos::read_from(&mut self.seq_map) {
            Ok(batch_res) => batch_res,
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
        assert_eq!(batch1.get_batch().get_last_offset(), 301);
        assert_eq!(batch1.get_pos(), 0);
    }

    #[fluvio_future::test]
    async fn test_batch_stream_multiple() {
        let test_dir = temp_dir().join("batch-stream-multiple");
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

        assert_eq!(batch_stream.get_pos(), 0);
        let batch1 = batch_stream.next().await.expect("batch");
        assert_eq!(batch1.get_batch().get_last_offset(), 301);
        assert_eq!(batch_stream.get_pos(), 79);
        let batch2 = batch_stream.next().await.expect("batch");
        assert_eq!(batch2.get_batch().get_last_offset(), 303);
    }
}
