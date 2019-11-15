// sink
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::mem;
use std::io::Error as IoError;
use std::task::Context;
use std::fmt;

use log::trace;
use log::debug;
use futures::io::AsyncWrite;
use futures::sink::Sink;
use futures::ready;
use futures::Future;
use std::task::Poll;
use pin_utils::unsafe_pinned;
use pin_utils::unsafe_unpinned;

use crate::fs::AsyncFile;
use crate::fs::AsyncFileSlice;
use crate::AsyncWrite2;

#[derive(Debug)]
pub enum FileSinkError {
    IoError(io::Error),
    MaxLenReached, // exceed max limit
}


impl fmt::Display for FileSinkError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::MaxLenReached => write!(f,"max len reached")
        }
    }
}



impl From<io::Error> for FileSinkError {
    fn from(error: io::Error) -> Self {
        FileSinkError::IoError(error)
    }
}

#[derive(Default)]
pub struct FileSinkOption {
    pub max_len: Option<u64>,
}

enum WriteState<B> {
    Ready,
    Received(B),
    Writing,
    Flush
}

impl <B>WriteState<B> {

    fn buffer(self) -> B  {
        match self {
            WriteState::Received(item) => item,
            _ => panic!("should only called when it is item")
        }
    }
}


/// File Sink
/// When starts, it calls write to produce future which write to file
/// future is stored in the write_store so it can be poll by consumer of the Sink
/// Since WriteAll is created in the function, it need to have different lifetime than struct itself
/// lifetime b can moved to method since it's no longer needed at Impl Sink Trait
/// parameter T is added so we can pass reference to [u8]
pub struct FileSink<B> {
    option: FileSinkOption,
    current_len: u64, // file size
    pending_len: u64,
    writer: AsyncFile,
    write_state: WriteState<B>,
}

impl<B> FileSink<B>  {
   
    pub async fn create<P>(path: P, option: FileSinkOption) -> Result<FileSink<B>, io::Error>
        where P: AsRef<Path>
    {
        let file = AsyncFile::create(path).await?;
        Ok(FileSink {
            writer: file,
            write_state: WriteState::Ready,
            current_len: 0,
            pending_len: 0,
            option,
        })
    }

    pub async fn open_write<P>(path: P, option: FileSinkOption) -> Result<FileSink<B>, io::Error>
        where P: AsRef<Path>
    {
        let file_path = path.as_ref();
        let file = AsyncFile::open(file_path).await?;
        let metadata = AsyncFile::get_metadata(file_path).await?;
        let len = metadata.len();

        Ok(FileSink {
            writer: file,
            write_state: WriteState::Ready,
            current_len: len,
            pending_len: 0,
            option,
        })
    }

    pub async fn open_append<P>(path: P, option: FileSinkOption) -> Result<FileSink<B>, io::Error>
        where P: AsRef<Path>
    {
        let file_path = path.as_ref();
        let file = AsyncFile::open_read_append(file_path).await?;
        let metadata = AsyncFile::get_metadata(file_path).await?;
        let len = metadata.len();

        Ok(FileSink {
            writer: file,
            write_state: WriteState::Ready,
            current_len: len,
            pending_len: 0,
            option,
        })
    }


    pub fn get_mut_writer(&mut self) -> &mut AsyncFile {
        &mut self.writer
    }

    pub fn get_writer(&self) -> &AsyncFile {
        &self.writer
    }

    pub async fn create_reader(&self) -> Result<AsyncFile,IoError> {
        self.writer.read_clone().await
    }

    pub fn get_current_len(&self) -> u64 {
        self.current_len
    }

    pub fn get_pending_len(&self) -> u64 {
        self.pending_len
    }

    pub async fn clone_writer(&self) -> Result<AsyncFile,IoError> {
        self.writer.try_clone().await
    }

    pub fn slice_from(&self, position: u64, len: u64) -> Result<AsyncFileSlice,IoError> {

        Ok(self.writer.raw_slice(position ,len))

    }

    
}

impl <B> Unpin for FileSink<B> where B: AsRef<[u8]>{}


impl<B> FileSink<B>  where B: AsRef<[u8]> {
    unsafe_pinned!(writer: AsyncFile);
    unsafe_unpinned!(write_state: WriteState<B>);
    unsafe_unpinned!(pending_len: u64);
    unsafe_unpinned!(current_len: u64);

  
    // write buffer if available
    fn try_empty_write(mut self: Pin<&mut Self>,cx: &mut Context,
    ) -> Poll<Result<(), FileSinkError>> {
        trace!(
            "write buf current len: {}, pending len: {}",
            self.current_len,
            self.pending_len
        );
        match self.as_mut().write_state() {
            WriteState::Received(_) => {
                let item = mem::replace(self.as_mut().write_state(),WriteState::Writing);
                trace!("pending write available, polling write");
                let mut writer = self.as_mut().writer();
                let mut write_state = writer.write_buf_all(item.buffer());
                let pin_state = unsafe { Pin::new_unchecked(&mut write_state)};
                ready!(pin_state.poll(cx))?;
                mem::replace(self.as_mut().write_state(),WriteState::Flush);
                Poll::Ready(Ok(()))
            },
            _ => panic!("sink is not received mode ")
        }
    }

    fn send_private(mut self: Pin<&mut Self>, item: B) -> Result<(), FileSinkError> where B: AsRef<[u8]> {
        assert_eq!(
            self.pending_len, 0,
            "pending len should always be 0 when start sending"
        );
        let item_ref = item.as_ref();
        let len = item_ref.len() as u64;
        trace!("start writing bytes len: {}, file len: {}", len,self.current_len);
        if let Some(max_len) = self.option.max_len {
            if self.current_len + len > max_len {
                debug!("pending will exceed max: {}",max_len);
                return Err(FileSinkError::MaxLenReached);
            }
        }
    

        *self.as_mut().pending_len() = len;
        trace!("set to received mode");
        mem::replace(self.as_mut().write_state(),WriteState::Received(item));
        Ok(())
    
       
    }


}

impl<B> Sink<B> for FileSink<B> where B:AsRef<[u8]> + Sync + Send {

    type Error = FileSinkError;

    fn poll_ready( self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        match self.write_state {
            WriteState::Ready => Poll::Ready(Ok(())),
            _ => Poll::Pending
        }
    }

    fn start_send(self: Pin<&mut Self>, item: B) -> Result<(), Self::Error> {
        self.send_private(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        // do lots of conv between pinned self and regular pinned
        // maybe this could be solved with unpin?
        if let Poll::Ready(Err(e)) = self.as_mut().try_empty_write(cx) {
            return Poll::Ready(Err(e.into()));
        }
        let result = self.as_mut().writer().poll_flush(cx).map_err(|err| err.into());
        match result {
            Poll::Ready(Ok(_)) => {
                let current_len = self.current_len + self.pending_len;
                *(self.as_mut().current_len())= current_len;
                *(self.as_mut().pending_len()) = 0;
                mem::replace(self.as_mut().write_state(),WriteState::Ready);
                trace!("flush, new len: {}",self.current_len);
                Poll::Ready(Ok(()))
            }
            _ => result,
        }
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {

    use futures::sink::SinkExt;
    use std::env::temp_dir;
    use std::fs::remove_file;
    use std::fs::File;
    use log::debug;
    use std::io::Read;
    use std::path::PathBuf;
    use future_helper::test_async;

    use super::FileSink;
    use super::FileSinkError;
    use super::FileSinkOption;


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
    async fn test_sink_file_write_happy_path()  -> Result<(),FileSinkError> {
        let test_file = temp_dir().join(TEST_FILE_NAME);
        ensure_clean_file(&test_file);

        let mut f_sink = FileSink::create(&test_file, FileSinkOption::default()).await?;

        let bytes = vec![0x01, 0x02, 0x03];
        f_sink.send(bytes).await?;
    
        let test_file = temp_dir().join(TEST_FILE_NAME);
        let mut f = File::open(test_file)?;
        let mut buffer = vec![0; 3];
        f.read(&mut buffer)?;
        assert_eq!(buffer[0], 0x01);
        assert_eq!(buffer[1], 0x02);
        assert_eq!(buffer[2], 0x03);
        Ok(())
    }

    /// example of async test
    #[test_async]
    async fn test_sink_file_max_reached() -> Result<(), FileSinkError> {
       
        let test_file = temp_dir().join(MAX_TEST_FILE_NAME);
        ensure_clean_file(&test_file);

        let option = FileSinkOption { max_len: Some(10) };

        let mut f_sink = FileSink::create(&test_file, option).await?;

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
