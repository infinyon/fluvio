/// async file that can be shared
use std::io::Error as IoError;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::task::Context;
use std::io::SeekFrom;

use log::trace;

use futures::io::AsyncRead;

use std::task::Poll;
use futures::Future;
use pin_utils::unsafe_unpinned;
use pin_utils::pin_mut;

use super::AsyncFile;

type InnerFile = Arc<RwLock<AsyncFile>>;

#[derive(Clone)]
pub struct SharedAsyncFile {
    inner: InnerFile,
}

impl SharedAsyncFile {
    fn new(file: AsyncFile) -> Self {
        SharedAsyncFile {
            inner: Arc::new(RwLock::new(file)),
        }
    }

    fn read<'a>(&self, buf: &'a mut [u8]) -> SharedAsyncFileRead<'a> {
        SharedAsyncFileRead {
            inner: self.inner.clone(),
            buf,
        }
    }

    pub fn seek(&self, pos: SeekFrom) -> SharedSeekFuture {
        SharedSeekFuture::new(self.inner.clone(), pos)
    }
}

impl AsyncRead for SharedAsyncFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, IoError>> {
        self.inner.write().unwrap().poll_read(cx, buf)
    }
}

impl From<AsyncFile> for SharedAsyncFile {
    fn from(file: AsyncFile) -> Self {
        SharedAsyncFile::new(file)
    }
}

pub struct SharedSeekFuture {
    inner: InnerFile,
    seek: SeekFrom,
}

impl Unpin for SharedSeekFuture {}

impl SharedSeekFuture {
    fn new(file: InnerFile, seek: SeekFrom) -> Self {
        SharedSeekFuture { inner: file, seek }
    }
}

impl Future for SharedSeekFuture {
    type Output = Result<u64, IoError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        trace!("reading  bytes");
        let this = &mut *self;
        let mut inner = this.inner.write().unwrap();
        let ft = inner.seek(this.seek.clone());
        pin_mut!(ft);
        ft.poll(cx)
    }
}

/// Based on Futures Read struct
/// Read future on shared file
/// Only allow one read at time using lock
pub struct SharedAsyncFileRead<'a> {
    inner: InnerFile,
    buf: &'a mut [u8],
}

impl<'a> SharedAsyncFileRead<'a> {
    unsafe_unpinned!(buf: &'a mut [u8]);

    fn new<'b: 'a>(inner: InnerFile, buf: &'b mut [u8]) -> Self {
        Self {
            inner: inner.clone(),
            buf,
        }
    }
}

impl Unpin for SharedAsyncFileRead<'_> {}

impl Future for SharedAsyncFileRead<'_> {
    type Output = Result<usize, IoError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        trace!("reading  bytes");
        let this = &mut *self;
        let mut inner = this.inner.write().unwrap();
        inner.poll_read(cx, this.buf)
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::io::Error as IoError;
    use std::io::SeekFrom;
    use futures::io::AsyncWriteExt;
    use futures::io::AsyncReadExt;

    use flv_future_core::test_async;
    use utils::fixture::ensure_clean_file;
    use crate::fs::AsyncFile;
    use super::SharedAsyncFile;

    #[test_async]
    async fn test_shared_read() -> Result<(), IoError> {
        let test_file_path = temp_dir().join("shared_read");
        ensure_clean_file(&test_file_path);

        let mut file = AsyncFile::create(&test_file_path).await?;
        file.write_all(b"test").await?;

        let mut buffer = [0; 4];
        let read_file = AsyncFile::open(&test_file_path).await?;
        let shared_file = SharedAsyncFile::new(read_file);
        let read_len = shared_file.read(&mut buffer).await?;
        assert_eq!(read_len, 4);
        let contents = String::from_utf8(buffer.to_vec()).expect("conversion");
        assert_eq!(contents, "test");

        let mut output = Vec::new();
        let mut file2 = shared_file.clone();
        file2.seek(SeekFrom::Start(0)).await?;
        file2.read_to_end(&mut output).await?;
        let contents = String::from_utf8(output).expect("conversion");
        assert_eq!(contents, "test");

        Ok(())
    }
}
