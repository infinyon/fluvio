// sink
use std::mem;
use std::io;
use std::io::Error as IoError;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::fmt;

use log::trace;
use log::debug;
use log::error;
use futures::ready;
use futures::Future;
use futures::sink::Sink;
use futures::io::AsyncWrite;
use pin_utils::unsafe_pinned;
use pin_utils::unsafe_unpinned;
use async_std::fs::metadata;
use async_std::fs::File;

use crate::fs::AsyncFile;
use crate::fs::AsyncFileSlice;
use crate::fs::file_util;
use crate::AsyncWrite2;


#[derive(Debug)]
pub enum BoundedFileSinkError {
    IoError(io::Error),
    MaxLenReached, // exceed max limit
}

impl fmt::Display for BoundedFileSinkError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::MaxLenReached => write!(f, "max len reached"),
        }
    }
}

impl From<io::Error> for BoundedFileSinkError {
    fn from(error: io::Error) -> Self {
        BoundedFileSinkError::IoError(error)
    }
}

#[derive(Default)]
pub struct BoundedFileOption {
    pub max_len: Option<u64>,
}

enum WriteState<B> {
    Ready,
    Received(B),
    Flush,
}

impl<B> WriteState<B> {
    fn buffer(self) -> B {
        match self {
            Self::Received(item) => item,
            _ => panic!("should only called when it is item"),
        }
    }
}

impl <B>fmt::Display for WriteState<B> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Received(_) => write!(f, "Received"),
            Self::Ready => write!(f, "Ready"),
            Self::Flush => write!(f,"Flush")
        }
    }
}

/// File Sink that only allows maximum bytes
/// When starts, it calls write to produce future which write to file
/// future is stored in the write_store so it can be poll by consumer of the Sink
/// Since WriteAll is created in the function, it need to have different lifetime than struct itself
/// lifetime b can moved to method since it's no longer needed at Impl Sink Trait
/// parameter T is added so we can pass reference to [u8]
pub struct BoundedFileSink<B> {
    option: BoundedFileOption,
    current_len: u64,
    pending_len: u64,
    writer: File,
    write_state: WriteState<B>,
    path: PathBuf,
}

impl<B> BoundedFileSink<B> {
    #[allow(unused)]
    pub async fn create<P>(path: P, option: BoundedFileOption) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let inner_path = path.as_ref();
        let writer = file_util::create(inner_path).await?;
        Ok(Self {
            writer,
            write_state: WriteState::Ready,
            path: inner_path.to_owned(),
            current_len: 0,
            pending_len: 0,
            option,
        })
    }

    #[allow(unused)]
    pub async fn open_write<P>(path: P, option: BoundedFileOption) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let file_path = path.as_ref();
        let writer = file_util::open(file_path).await?;
        let metadata = metadata(file_path).await?;
        let len = metadata.len();

        Ok(Self {
            writer,
            write_state: WriteState::Ready,
            path: file_path.to_owned(),
            current_len: len,
            pending_len: 0,
            option,
        })
    }

    #[allow(unused)]
    pub async fn open_append<P>(path: P, option: BoundedFileOption) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let file_path = path.as_ref();
        let writer = file_util::open_read_append(file_path).await?;
        let metadata = metadata(file_path).await?;
        let len = metadata.len();

        Ok(Self {
            writer,
            write_state: WriteState::Ready,
            path: file_path.to_owned(),
            current_len: len,
            pending_len: 0,
            option,
        })
    }

    #[allow(unused)]
    pub fn get_current_len(&self) -> u64 {
        self.current_len
    }

    pub fn get_pending_len(&self) -> u64 {
        self.pending_len
    }

    pub fn get_path(&self) -> &Path {
        &self.path
    }

    pub fn inner(&self) -> &File {
        &self.writer
    }

    pub fn mut_inner(&mut self) -> &mut File {
        &mut self.writer
    }

    #[allow(unused)]
    pub fn slice_from(&self, position: u64, len: u64) -> Result<AsyncFileSlice, IoError> {
        Ok(self.writer.raw_slice(position, len))
    }
}

impl<B> Unpin for BoundedFileSink<B> where B: AsRef<[u8]> {}

impl<B> BoundedFileSink<B>
where
    B: AsRef<[u8]>,
{
    unsafe_pinned!(writer: File);
    unsafe_unpinned!(write_state: WriteState<B>);
    unsafe_unpinned!(pending_len: u64);
    unsafe_unpinned!(current_len: u64);

    // if we have received item, try to write it
    fn try_empty_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<(), BoundedFileSinkError>> {
        trace!(
            "trying write buf current len: {}, pending len: {}, state: {}",
            self.current_len,
            self.pending_len,
            self.write_state
        );
        match self.as_mut().write_state() {
            WriteState::Received(_) => {
                trace!("pending write available, performing write buf all");
                let prev_state = mem::replace(self.as_mut().write_state(), WriteState::Flush);
                let mut writer = self.as_mut().writer();
                let mut write_state = writer.write_buf_all(prev_state.buffer());
                let pin_state = unsafe { Pin::new_unchecked(&mut write_state) };
                // since we are using write buf all, either buffer has written completely or error out
                ready!(pin_state.poll(cx))?;
                Poll::Ready(Ok(()))
            },
            WriteState::Flush => {
                trace!("write state is flush mode, no more write needed");
                Poll::Ready(Ok(()))
            },
            WriteState::Ready => {
                trace!("write is ready, ready");
                Poll::Ready(Ok(()))
            },
        }
    }

    fn send_private(mut self: Pin<&mut Self>, item: B) -> Result<(), BoundedFileSinkError>
    where
        B: AsRef<[u8]>,
    {
        assert_eq!(
            self.pending_len, 0,
            "pending len should always be 0 when start sending"
        );
        let item_ref = item.as_ref();
        let len = item_ref.len() as u64;
        trace!(
            "start writing bytes len: {}, file len: {}",
            len,
            self.current_len
        );
        if let Some(max_len) = self.option.max_len {
            if self.current_len + len > max_len {
                debug!("pending write will exceed max: {}", max_len);
                return Err(BoundedFileSinkError::MaxLenReached);
            }
        }

        *self.as_mut().pending_len() = len;
        trace!("set to received mode");
        mem::replace(self.as_mut().write_state(), WriteState::Received(item));
        Ok(())
    }
}

impl<B> Sink<B> for BoundedFileSink<B>
where
    B: AsRef<[u8]> + Sync + Send,
{
    type Error = BoundedFileSinkError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        match self.write_state {
            WriteState::Ready => {
                trace!("poll ready: write state ready => ready");
                Poll::Ready(Ok(()))
            },
            _ => {
                trace!("poll ready with write state: {} => pending ",self.write_state);
                Poll::Pending
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: B) -> Result<(), Self::Error> {
        trace!("start sending");
        self.send_private(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        // do lots of conv between pinned self and regular pinned
        // maybe this could be solved with unpin?
        if let Poll::Ready(Err(e)) = self.as_mut().try_empty_write(cx) {
            return Poll::Ready(Err(e.into()));
        }
        trace!("starting poll flush");
        let result = self
            .as_mut()
            .writer()
            .poll_flush(cx)
            .map_err(|err| err.into());
        match result {
            Poll::Ready(Ok(_)) => {
                trace!("flush success");
                let current_len = self.current_len + self.pending_len;
                *(self.as_mut().current_len()) = current_len;
                *(self.as_mut().pending_len()) = 0;
                mem::replace(self.as_mut().write_state(), WriteState::Ready);
                trace!("flush, new len: {}", self.current_len);
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                trace!("flush still pending");
                Poll::Pending
            }
            _ => {
                error!("error flushing: {:#?}", result);
                result
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        trace!("poll close");
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::fs::remove_file;
    use std::fs::File;
    use std::io::Read;
    use std::path::PathBuf;

    use log::debug;
    use futures::sink::SinkExt;

    use future_helper::test_async;

    use super::BoundedFileSink;
    use super::BoundedFileOption;
    use super::BoundedFileSinkError;

    const TEST_FILE_NAME: &str = "file_test_01";
    const MAX_TEST_FILE_NAME: &str = "file_test_max";

    fn ensure_clean_file(log_path: &PathBuf) {
        debug!("removing log: {}", log_path.display());
        // delete message log if it exists
        if let Ok(_) = remove_file(log_path) {
            debug!("remove existing log file");
        } else {
            debug!("no existing log file");
        }
    }

    #[test_async]
    async fn test_sink_file_write_happy_path() -> Result<(), BoundedFileSinkError> {
        let test_file = temp_dir().join(TEST_FILE_NAME);
        ensure_clean_file(&test_file);

        let mut f_sink = BoundedFileSink::create(&test_file, BoundedFileOption::default()).await?;

        let bytes = vec![0x01, 0x02, 0x03];
        f_sink.send(&bytes).await?;
        let test_file = temp_dir().join(TEST_FILE_NAME);
        let mut f = File::open(test_file)?;
        let mut buffer = vec![0; 3];
        f.read(&mut buffer)?;
        assert_eq!(buffer[0], 0x01);
        assert_eq!(buffer[1], 0x02);
        assert_eq!(buffer[2], 0x03);
        Ok(())
    }

    const TEST_FILE_NAME2: &str = "file_test_02";

    #[test_async]
    async fn test_sink_file_write_multiple_path() -> Result<(), BoundedFileSinkError> {
        let test_file = temp_dir().join(TEST_FILE_NAME2);
        ensure_clean_file(&test_file);

        let mut f_sink = BoundedFileSink::create(&test_file, BoundedFileOption::default()).await?;

        let bytes = vec![0x1;1000];
        f_sink.send(&bytes).await?;
        f_sink.send(&bytes).await?;
        let test_file = temp_dir().join(TEST_FILE_NAME2);
        let mut f = File::open(test_file)?;
        let mut buffer = vec![0;2000];
        let len = f.read(&mut buffer)?;
        assert_eq!(len,2000);
        Ok(())
    }


    /// example of async test
    #[test_async]
    async fn test_sink_file_max_reached() -> Result<(), BoundedFileSinkError> {
        let test_file = temp_dir().join(MAX_TEST_FILE_NAME);
        ensure_clean_file(&test_file);

        let option = BoundedFileOption { max_len: Some(10) };

        let mut f_sink = BoundedFileSink::create(&test_file, option).await?;

        let bytes = vec![0x01; 8];
        // first send let should be 8
        debug!("====> first write =====");
        f_sink.send(bytes.clone()).await?;
        assert_eq!(f_sink.current_len, 8);
        assert_eq!(f_sink.pending_len, 0);
        debug!("=====> second write ====");
        // should fail at this point because there isn't enough capacity
        let res = f_sink.send(bytes).await;
        assert!(res.is_err(), "should reached max");
        Ok(())
    }
}
