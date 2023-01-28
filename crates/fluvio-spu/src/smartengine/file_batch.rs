use std::os::unix::io::RawFd;
use std::io::{Error as IoError, ErrorKind, Cursor};

use tracing::{warn, debug};
use nix::sys::uio::pread;

use fluvio_protocol::record::{Batch, BATCH_FILE_HEADER_SIZE, BATCH_HEADER_SIZE};
use fluvio_protocol::record::Offset;
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
    #[allow(unused)]
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
            end: offset + slice.len() as i64,
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
            .map_err(|err| IoError::new(ErrorKind::Other, format!("pread error {err}")))
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

        let mut batch: Batch = Batch::default();
        if let Err(err) = batch.decode_from_file_buf(&mut Cursor::new(header), 0) {
            return Some(Err(IoError::new(
                ErrorKind::Other,
                format!("decodinge batch header error {err}"),
            )));
        }

        let remainder = batch.batch_len as usize - BATCH_HEADER_SIZE;

        debug!(
            file_offset = self.offset,
            base_offset = batch.base_offset,
            "fbatch header"
        );

        let mut raw_records = vec![0u8; remainder];

        self.offset += BATCH_FILE_HEADER_SIZE as i64;

        let bytes_read = match pread(self.fd, &mut raw_records, self.offset)
            .map_err(|err| IoError::new(ErrorKind::Other, format!("pread error {err}")))
        {
            Ok(bytes) => bytes,
            Err(err) => return Some(Err(err)),
        };

        if bytes_read < raw_records.len() {
            warn!(bytes_read, record_len = raw_records.len());
            return Some(Err(IoError::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not enough for batch records {} out of {}",
                    bytes_read,
                    raw_records.len()
                ),
            )));
        }

        let compression = match batch.get_compression() {
            Ok(compression) => compression,
            Err(err) => {
                return Some(Err(IoError::new(
                    ErrorKind::Other,
                    format!("unknown compression value for batch {err}"),
                )))
            }
        };

        let records = match compression.uncompress(&raw_records) {
            Ok(Some(records)) => records,
            Ok(None) => raw_records,
            Err(err) => {
                return Some(Err(IoError::new(
                    ErrorKind::Other,
                    format!("uncompress error {err}"),
                )))
            }
        };

        self.offset += bytes_read as i64;

        debug!(file_offset = self.offset, "fbatch end");

        Some(Ok(FileBatch { batch, records }))
    }
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::Write};
    use std::env::temp_dir;
    use std::os::unix::io::AsRawFd;

    use fluvio_types::defaults::STORAGE_MAX_BATCH_SIZE;

    use super::*;

    #[test]
    fn test_file() {
        let path = temp_dir().join("pread.txt");
        let mut file = File::create(&path).expect("create");
        file.write_all(b"Hello, world!").expect("write");
        file.sync_all().expect("flush");
        drop(file);

        let read_only = File::open(path).expect("open");
        let fd = read_only.as_raw_fd();
        let mut buf = vec![0; STORAGE_MAX_BATCH_SIZE as usize];
        // let mut buf = BytesMut::with_capacity(64);
        let bytes_read = pread(fd, &mut buf, 1).expect("");
        println!("bytes read: {bytes_read}");
        assert!(bytes_read > 2);
    }
}
