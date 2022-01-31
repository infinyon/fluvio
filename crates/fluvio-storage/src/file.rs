use std::os::unix::prelude::{RawFd, AsRawFd};
use std::path::Path;
use std::io::{Error as IoError, ErrorKind};

use async_trait::async_trait;
use blocking::unblock;
use bytes::BytesMut;
use dataplane::Size;
use fluvio_future::fs::File;
use libc::{c_void};
use nix::errno::Errno;
use nix::Result as NixResult;

use crate::batch::StorageBytesIterator;

pub struct AsyncFile(File);

/// File based iterator
pub struct FileBytesIterator {
    file: File,
    pos: Size,
    buf: BytesMut,
}

#[async_trait]
impl StorageBytesIterator for FileBytesIterator {
    async fn open<P: AsRef<Path> + Send>(path: P) -> Result<Self, IoError> {
        let file = File::open(path).await?;
        Ok(Self {
            file,
            pos: 0,
            buf: BytesMut::with_capacity(1024),
        })
    }

    fn get_pos(&self) -> Size {
        self.pos
    }

    async fn read_bytes(&mut self, len: Size) -> Result<(&[u8], Size), IoError> {
        self.buf.resize(len as usize, 0);
        let fd = AsyncFileDescriptor(self.file.as_raw_fd());
        let len = fd
            .pread(self.buf.clone(), self.pos as i64, len as usize)
            .await
            .map_err(|e| IoError::new(ErrorKind::Other, format!("pread error: {:#?}", e)))?;
        Ok((self.buf.as_ref(), len as Size))
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
struct AsyncFileDescriptor(RawFd);

impl AsyncFileDescriptor {
    /// read number of bytes into shared buffer at offset
    async fn pread(&self, mut buf: BytesMut, offset: i64, len: usize) -> NixResult<usize> {
        let fd = self.0;
        unblock(move || {
            let buf = buf.as_mut();
            let mut buf_len = len;
            let mut buf_offset = 0;
            let mut total_read = 0;
            while buf_len > 0 {
                let res = unsafe {
                    libc::pread(
                        fd,
                        buf.as_mut_ptr().offset(buf_offset) as *mut c_void,
                        buf.len(),
                        offset + total_read as i64,
                    )
                };

                let read = Errno::result(res).map(|r| r as isize)?;
                buf_len -= read as usize;
                buf_offset += read;
                total_read += read;
            }
            Ok(total_read as usize)
        })
        .await
    }
}
