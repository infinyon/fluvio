use dataplane::batch::{Batch, BATCH_FILE_HEADER_SIZE, BATCH_HEADER_SIZE};
use dataplane::Offset;
use std::io::{Error as IoError, ErrorKind, Cursor};
use tracing::{warn, debug};
use std::os::unix::io::RawFd;
use nix::sys::uio::pread;
use fluvio_future::file_slice::AsyncFileSlice;

// only encode information necessary to decode batches efficiently
pub struct FileBatch {
    pub(crate) batch: Batch,
    pub(crate) records: Vec<u8>,
}

impl FileBatch {
    pub(crate) fn base_offset(&self) -> Offset {
        self.batch.base_offset
    }

    pub(crate) fn offset_delta(&self) -> i32 {
        self.batch.header.last_offset_delta
    }
}

/// Iterator that returns batch from file
pub struct FileBatchIterator {
    fd: RawFd,
    offset: i64,
    end: i64,
}

impl FileBatchIterator {
    pub fn new(fd: RawFd, offset: i64, len: i64) -> Self {
        Self {
            fd,
            offset,
            end: offset + len,
        }
    }

    pub fn from_raw_slice(slice: AsyncFileSlice) -> Self {
        use std::os::unix::io::AsRawFd;
        let offset = slice.position() as i64;
        Self {
            fd: slice.as_raw_fd(),
            offset,
            end: offset + slice.len(),
        }
    }
}

impl Iterator for FileBatchIterator {
    type Item = Result<FileBatch, IoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.end {
            return None;
        }

        let mut header = vec![0u8; BATCH_FILE_HEADER_SIZE];
        let bytes_read = match pread(self.fd, &mut header, self.offset)
            .map_err(|err| IoError::new(ErrorKind::Other, format!("pread error {}", err)))
        {
            Ok(bytes) => bytes,
            Err(err) => return Some(Err(err)),
        };

        if bytes_read < header.len() {
            warn!(bytes_read, header_len = header.len());
            return Some(Err(IoError::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not eough for batch header {} out of {}",
                    bytes_read,
                    header.len()
                ),
            )));
        }

        let mut batch = Batch::default();
        if let Err(err) = batch.decode_from_file_buf(&mut Cursor::new(header), 0) {
            return Some(Err(IoError::new(
                ErrorKind::Other,
                format!("decodinge batch header error {}", err),
            )));
        }

        let remainder = batch.batch_len as usize - BATCH_HEADER_SIZE as usize;

        debug!(
            file_offset = self.offset,
            base_offset = batch.base_offset,
            "fbatch header"
        );

        let mut records = vec![0u8; remainder];

        self.offset += BATCH_FILE_HEADER_SIZE as i64;

        let bytes_read = match pread(self.fd, &mut records, self.offset)
            .map_err(|err| IoError::new(ErrorKind::Other, format!("pread error {}", err)))
        {
            Ok(bytes) => bytes,
            Err(err) => return Some(Err(err)),
        };

        if bytes_read < records.len() {
            warn!(bytes_read, record_len = records.len());
            return Some(Err(IoError::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not enough for batch records {} out of {}",
                    bytes_read,
                    records.len()
                ),
            )));
        }

        self.offset += bytes_read as i64;

        debug!(file_offset = self.offset, "fbatch end");

        Some(Ok(FileBatch { batch, records }))
    }
}
