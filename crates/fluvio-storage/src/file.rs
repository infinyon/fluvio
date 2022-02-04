use std::os::unix::prelude::{RawFd, AsRawFd};
use std::path::Path;
use std::io::{Error as IoError, ErrorKind};

use async_trait::async_trait;
use blocking::unblock;
use bytes::{BytesMut, Bytes};
use dataplane::Size;

use libc::{c_void};
use nix::errno::Errno;
use nix::Result as NixResult;
use tracing::{debug, instrument, trace};

use fluvio_future::fs::File;

use crate::batch::StorageBytesIterator;

pub struct AsyncFile(File);

/// File based iterator
pub struct FileBytesIterator {
    file: File,
    pos: Size,
}

#[async_trait]
impl StorageBytesIterator for FileBytesIterator {
    async fn open<P: AsRef<Path> + Send>(path: P) -> Result<Self, IoError> {
        debug!(path = ?path.as_ref().display(),"open file");
        let file = File::open(path).await?;
        Ok(Self { file, pos: 0 })
    }

    fn get_pos(&self) -> Size {
        self.pos
    }

    #[instrument(skip(self),level = "trace",fields(pos = self.pos))]
    async fn read_bytes(&mut self, len: Size) -> Result<Bytes, IoError> {
        let fd = AsyncFileDescriptor(self.file.as_raw_fd());
        let buffer = fd
            .pread(self.pos as i64, len as usize)
            .await
            .map_err(|e| IoError::new(ErrorKind::Other, format!("pread error: {:#?}", e)))?;
        trace!(len = buffer.len(), "read bytes");

        Ok(buffer)
    }

    async fn seek(&mut self, amount: Size) -> Result<Size, IoError> {
        self.pos += amount;
        Ok(amount)
    }

    async fn set_absolute(&mut self, offset: Size) -> Result<Size, IoError> {
        self.pos = offset;
        Ok(offset)
    }
}

/// Async File Descriptor
#[derive(Debug)]
struct AsyncFileDescriptor(RawFd);

impl AsyncFileDescriptor {
    /// read number of bytes into shared buffer at offset
    #[instrument(skip(self),level = "trace",fields(fd = self.0, offset, len))]
    async fn pread(&self, offset: i64, len: usize) -> NixResult<Bytes> {
        let fd = self.0;
        unblock(move || {
            let mut buf = BytesMut::with_capacity(len as usize);
           // buf.resize(len as usize, 0);
            let mut buf_len = len;
            let mut buf_offset = 0;
            let mut total_read = 0;
            while buf_len > 0 {
                trace!(buf_len, buf_offset, total_read, "pread start");
                let res = unsafe {
                    libc::pread(
                        fd,
                        buf.as_mut_ptr().offset(buf_offset) as *mut c_void,
                        buf_len,
                        offset + total_read as i64,
                    )
                };
                let read = Errno::result(res).map(|r| r as isize)?;
                trace!(fd, read, "pread success");
                buf_len -= read as usize;
                buf_offset += read;
                total_read += read;
            }
            unsafe { buf.set_len(total_read as usize) };
            Ok(buf.freeze())
        })
        .await
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;

    use futures_lite::AsyncWriteExt;

    use fluvio_future::{fs::File};

    use super::*;

    #[fluvio_future::test]
    async fn test_read_bytes() {
        let test_file = temp_dir().join("simple_write");

        let mut file = File::create(&test_file).await.expect("file creation");
        file.write_all(b"hello world").await.expect("write");
        file.flush().await.expect("flush");
        drop(file);

        let mut iter = FileBytesIterator::open(&test_file)
            .await
            .expect("open test");
        let word = iter.read_bytes(5).await.expect("read bytes");
        assert_eq!(word.len(), 5);
        assert_eq!(word.as_ref(), b"hello");
    }
}
