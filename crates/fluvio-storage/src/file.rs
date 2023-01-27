use std::os::unix::prelude::{RawFd, AsRawFd};
use std::path::Path;
use std::io::{Error as IoError, ErrorKind};

use async_trait::async_trait;
use blocking::unblock;
use bytes::{BytesMut, Bytes};
use fluvio_protocol::record::Size;

use libc::{c_void};
use nix::errno::Errno;
use nix::Result as NixResult;
use tracing::{debug, instrument, trace};

use fluvio_future::fs::File;

use crate::batch::StorageBytesIterator;

/// File based iterator
pub struct FileBytesIterator {
    file: File,
    pos: Size,
    end: bool,
}

#[async_trait]
impl StorageBytesIterator for FileBytesIterator {
    async fn open<P: AsRef<Path> + Send>(path: P) -> Result<Self, IoError> {
        debug!(path = ?path.as_ref().display(),"opening log file for iteration");
        let file = File::open(path).await?;
        Self::from_file(file).await
    }

    async fn from_file(file: File) -> Result<Self, IoError> {
        Ok(Self {
            file,
            pos: 0,
            end: false,
        })
    }

    fn get_pos(&self) -> Size {
        self.pos
    }

    #[instrument(skip(self),level = "trace",fields(pos = self.pos))]
    async fn read_bytes(&mut self, len: Size) -> Result<Option<Bytes>, IoError> {
        let fd = self.file.as_raw_fd();

        // this will block task which will make it harder to scale
        // this should be replaced with async implementation
        let file_pos = self.pos as i64;
        match unblock(move || pread(fd, file_pos, len as usize))
            .await
            .map_err(|e| IoError::new(ErrorKind::Other, format!("pread error: {e:#?}")))?
        {
            ReadOutput::Some { buffer, eof } => {
                trace!(len = buffer.len(), "read bytes");
                self.pos += len;
                self.end = eof;
                Ok(Some(buffer))
            }
            ReadOutput::Empty => {
                self.end = true;
                Ok(None)
            }
        }
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

enum ReadOutput {
    Empty, // EOF, no bytes read
    Some { buffer: Bytes, eof: bool },
}

impl ReadOutput {
    #[allow(unused)]
    pub fn expect(self, msg: &str) -> (Bytes, bool) {
        match self {
            Self::Some { buffer, eof } => (buffer, eof),
            Self::Empty => panic!("{msg}"),
        }
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Some { .. } => false,
            Self::Empty => true,
        }
    }
}

/// read number of bytes into shared buffer at offset
#[instrument(level = "trace", fields(fd, offset, len))]
fn pread(fd: RawFd, offset: i64, len: usize) -> NixResult<ReadOutput> {
    let mut eof = false;
    let mut buf = BytesMut::with_capacity(len);
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
        let read = Errno::result(res)?;
        if read == 0 {
            trace!(fd, total_read, "end of file");
            if total_read == 0 {
                return Ok(ReadOutput::Empty);
            } else {
                eof = true;
                break;
            }
        } else {
            buf_len -= read as usize;
            buf_offset += read;
            total_read += read;
            trace!(fd, read, buf_len, buf_offset, total_read, "pread success");
        }
    }
    unsafe { buf.set_len(total_read as usize) };
    Ok(ReadOutput::Some {
        buffer: buf.freeze(),
        eof,
    })
}

/*
mod mem {

    use std::cmp::min;
    use std::fs::OpenOptions;

    use memmap::Mmap;

    /// Memory mapped based iterator
    pub struct MmapBytesIterator {
        map: Mmap,
        pos: Size,
    }

    impl MmapBytesIterator {}

    #[async_trait]
    impl StorageBytesIterator for MmapBytesIterator {
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

        async fn read_bytes(&mut self, len: Size) -> Result<Option<Bytes>, IoError> {
            /*
                    // println!("inner len: {}, read_len: {}", self.map.len(),len);
            let bytes = (&self.map).split_at(self.pos as usize).1;
            let prev_pos = self.pos;
            self.pos = min(self.map.len() as Size, self.pos as Size + len);
            // println!("prev pos: {}, new pos: {}", prev_pos, self.pos);
            Ok((bytes, self.pos - prev_pos))
            */
            todo!()
        }

        // seek relative
        async fn seek(&mut self, amount: Size) -> Result<Size, IoError> {
            self.pos = min(self.map.len() as Size, self.pos as Size + amount);
            Ok(amount)
        }

        // seek absolute
        async fn set_absolute(&mut self, offset: Size) -> Result<Size, IoError> {
            self.pos = min(self.map.len() as Size, offset);
            Ok(self.pos)
        }

        #[inline(always)]
        fn get_pos(&self) -> Size {
            self.pos
        }
    }
}
*/

/*
const DEFAULT_BUFFER_SIZE: usize = 4096;

/// File based iterator
pub struct BufferedFileBytesIterator {
    pub file: File,
    pub pos: Size,
    pub end: bool,
    pub read_count: usize,
    pub cache_hit: usize,
    buffer: Bytes,
}

#[async_trait]
impl StorageBytesIterator for BufferedFileBytesIterator {
    async fn open<P: AsRef<Path> + Send>(path: P) -> Result<Self, IoError> {
        debug!(path = ?path.as_ref().display(),"open file");
        let file = File::open(path).await?;
        Ok(Self {
            file: file,
            pos: 0,
            end: false,
            read_count: 0,
            cache_hit: 0,
            buffer: Bytes::new(),
        })
    }

    fn get_pos(&self) -> Size {
        self.pos
    }

    #[instrument(skip(self),level = "trace", fields(pos = self.pos))]
    async fn read_bytes(&mut self, len: Size) -> Result<Option<Bytes>, IoError> {


        // if we have enough bytes in buffer
        if self.buffer.len() >= len as usize {
            trace!(buf_len = self.buffer.len(), "in buffer");
            self.cache_hit += 1;
            let cached_buffer = self.buffer.split_to(len as usize);
            trace!(return_len = cached_buffer.len(), "split");
            return Ok(Some(cached_buffer));
        } else if self.end {
            if self.buffer.is_empty() {
                trace!("eof and empty");
                return Ok(None)
            } else {
                let cached_buffer = self.buffer.split_to(self.buffer.len());
                trace!(return_len = cached_buffer.len(),remain = self.buffer.len(),"eof and not empty");
                return Ok(Some(cached_buffer))
            }
        }

        let fd = self.file.as_raw_fd();

        match pread(fd, self.pos as i64, max(DEFAULT_BUFFER_SIZE, len as usize))
           // .await
            .map_err(|e| IoError::new(ErrorKind::Other, format!("pread error: {:#?}", e)))?
        {
            ReadOutput::Some { mut buffer, eof } => {
                trace!(len = buffer.len(), eof, "read bytes");
                self.pos += len;
                self.read_count += 1;
                self.end = eof;
                // only cache if we have enough bytes
                if buffer.len() < DEFAULT_BUFFER_SIZE as usize {
                    self.buffer = buffer.split_off(len as usize);
                    trace!(
                        return_len = buffer.len(),
                        remaining_len = self.buffer.len(),
                        "caching"
                    );
                    Ok(Some(buffer))
                } else {
                    self.buffer = Bytes::new();
                    Ok(Some(buffer))
                }
            }
            ReadOutput::Empty => {
                trace!("empty");
                self.end = true;
                self.buffer = Bytes::new();
                Ok(None)
            }
        }
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
*/

#[cfg(test)]
mod tests {

    use std::env::temp_dir;

    use futures_lite::AsyncWriteExt;

    use fluvio_future::{fs::File};

    use super::*;

    #[fluvio_future::test]
    async fn test_file_descriptor() {
        let test_file = temp_dir().join("simple_write");

        let mut file = File::create(&test_file).await.expect("file creation");
        file.write_all(b"hello world").await.expect("write");
        file.flush().await.expect("flush");
        drop(file);

        let file = File::open(&test_file).await.expect("open");
        let fd = file.as_raw_fd();

        let (word, eof) = pread(fd, 0, 5).expect("read bytes").expect("some");
        assert_eq!(word.len(), 5);
        assert_eq!(word.as_ref(), b"hello");
        assert!(!eof);
        let (word, eof) = pread(fd, 5, 20).expect("read bytes").expect("some");
        assert_eq!(word.len(), 6);
        assert_eq!(word.as_ref(), b" world");
        assert!(eof);
        // try to read past end of file
        assert!(pread(fd, 100, 10).expect("read bytes").is_empty());
    }

    #[fluvio_future::test]
    async fn test_file_iterator() {
        let test_file = temp_dir().join("simple_write");

        let mut file = File::create(&test_file).await.expect("file creation");
        file.write_all(b"hello world").await.expect("write");
        file.flush().await.expect("flush");
        drop(file);

        let mut iter = FileBytesIterator::open(&test_file)
            .await
            .expect("open test");
        let word = iter.read_bytes(5).await.expect("read bytes").expect("some");
        assert_eq!(word.len(), 5);
        assert_eq!(word.as_ref(), b"hello");

        // this should be cached and return fully
        let word = iter.read_bytes(3).await.expect("read bytes").expect("some");
        assert_eq!(word.len(), 3);
        assert_eq!(word.as_ref(), b" wo");

        // only 3 bytes are cached since we reached eof
        let word = iter.read_bytes(6).await.expect("read bytes").expect("some");
        assert_eq!(word.len(), 3);
        assert_eq!(word.as_ref(), b"rld");
    }
}
