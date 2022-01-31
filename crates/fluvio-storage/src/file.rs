use std::os::unix::prelude::RawFd;

use blocking::unblock;
use bytes::BytesMut;
use libc::{c_void, size_t};
use nix::errno::Errno;
use nix::Result;

/// Async File Descriptor
pub struct AsyncFileDescriptor {
    fd: RawFd,
}

impl AsyncFileDescriptor {

    /// read number of bytes into shared buffer at offset
    pub async fn pread(&self,mut buf: BytesMut, offset: i64,len: usize) -> Result<usize> {

        let fd = self.fd;
        unblock(move || {
            let buf = buf.as_mut();
            let mut buf_len = len;
            let mut buf_offset = 0;
            let mut total_read = 0;
            while buf_len > 0 {

                let res = unsafe {
                    libc::pread(fd, buf.as_mut_ptr().offset(buf_offset ) as *mut c_void, buf.len(),
                               offset + total_read as i64)
                };

                let read = Errno::result(res).map(|r| r as isize)?;
                buf_len -= read as usize;
                buf_offset += read;
                total_read += read;
            }
            Ok(total_read as usize)
        }).await
    }
}
