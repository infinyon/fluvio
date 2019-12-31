
use std::io;
use std::io::Error as IoError;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::fmt;

use log::trace;
use futures::io::AsyncWrite;
use pin_utils::unsafe_pinned;
use pin_utils::unsafe_unpinned;
use async_std::fs::metadata;
use async_std::fs::File;

use crate::fs::AsyncFile;
use crate::fs::AsyncFileSlice;
use crate::fs::file_util;


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



/// File Sink that tracks how much byte it has written
/// This file will not block write operation.  It is up to writer to check if maximum file has size is reached
/// since AsyncWrite return IoError 
pub struct BoundedFileSink {
    option: BoundedFileOption,
    current_len: u64,
    writer: File,
    path: PathBuf,
}

impl Unpin for BoundedFileSink  {}


impl BoundedFileSink {

    unsafe_pinned!(writer: File);
    unsafe_unpinned!(current_len: u64);

    #[allow(unused)]
    pub async fn create<P>(path: P, option: BoundedFileOption) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let inner_path = path.as_ref();
        let writer = file_util::create(inner_path).await?;
        Ok(Self {
            writer,
            path: inner_path.to_owned(),
            current_len: 0,
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
            path: file_path.to_owned(),
            current_len: len,
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
            path: file_path.to_owned(),
            current_len: len,
            option,
        })
    }

    #[allow(unused)]
    pub fn get_current_len(&self) -> u64 {
        self.current_len
    }

    /// check if buf_len can be written
    pub fn can_be_appended(&self,buf_len: u64) -> bool {
        match self.option.max_len {
            Some(max_len) => self.current_len + buf_len <= max_len,
            None => true
        }
        
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


impl AsyncWrite for BoundedFileSink {

    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {

        match self.as_mut().writer().poll_write(cx,buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                match result {
                    Ok(size) => {
                        let current_len = self.as_ref().current_len + size as u64;
                        *(self.as_mut().current_len()) = current_len;
                        trace!("success write: {}, current len: {}",size,self.as_ref().current_len);
                        Poll::Ready(Ok(size))
                    },
                    Err(err) => Poll::Ready(Err(err))
                }   
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.writer().poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.writer().poll_close(cx)
    }
}


#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::fs::remove_file;
    use std::fs::File as StdFile;
    use std::io::Read;
    use std::path::PathBuf;

    use log::debug;
    use futures::io::AsyncReadExt;
    use futures::io::AsyncWriteExt;
    use futures::io::AsyncSeekExt;
    use async_std::io::SeekFrom;

    use flv_future_core::test_async;


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
        f_sink.write_all(&bytes).await.expect("write bytes");
        assert_eq!(f_sink.get_current_len(),3);

        f_sink.flush().await.expect("flush");
       

        let test_file = temp_dir().join(TEST_FILE_NAME);
        let mut f = StdFile::open(test_file)?;
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

        let mut f_sink = BoundedFileSink::create(&test_file, BoundedFileOption::default()).await.expect("create");

        let bytes = vec![0x1; 1000];
        f_sink.write_all(&bytes).await.expect("first write");
        f_sink.write_all(&bytes).await.expect("second write");

        assert_eq!(f_sink.get_current_len(),2000);
        f_sink.flush().await.expect("flush");

        let test_file = temp_dir().join(TEST_FILE_NAME2);
        let mut f = StdFile::open(test_file).expect("test file should exists");
        let mut buffer = vec![0; 2000];
        let len = f.read(&mut buffer)?;
        assert_eq!(len, 2000);
        Ok(())
    }

    /// example of async test
    #[test_async]
    async fn test_sink_file_max_reached() -> Result<(), BoundedFileSinkError> {
        let test_file = temp_dir().join(MAX_TEST_FILE_NAME);
        ensure_clean_file(&test_file);

        let option = BoundedFileOption { max_len: Some(10) };

        let mut f_sink = BoundedFileSink::create(&test_file, option).await.expect("file created");

        let bytes = vec![0x01; 8];
        f_sink.write_all(&bytes).await.expect("first write");
        assert_eq!(f_sink.get_current_len(), 8);
        assert!(!f_sink.can_be_appended(20));
        Ok(())
    }



    #[test_async]
    async fn test_sink_file_write_read() -> Result<(), BoundedFileSinkError> {

        const WRITE_FILE: &str = "file_test_write_bounded";

        let test_file = temp_dir().join(WRITE_FILE);
        ensure_clean_file(&test_file);

        let mut f_sink = BoundedFileSink::open_append(&test_file, BoundedFileOption::default()).await?;

        let bytes: Vec<u8> = vec![0x01; 73];
        f_sink.write_all(&bytes).await.expect("send success");
        debug!("current len: {}",f_sink.get_current_len());
        let bytes: Vec<u8> = vec![0x01; 74];
        f_sink.write_all(&bytes).await.expect("send success");
        assert_eq!(f_sink.get_current_len(),147);


        // check if we read back
        f_sink.mut_inner().seek(SeekFrom::Start(0)).await.expect("reset to beginning");
        // now read back
        let mut read_buf: Vec<u8> = vec![];
        f_sink.mut_inner().read_to_end(&mut read_buf).await.expect("read");
        assert_eq!(read_buf.len(),147);
        
        Ok(())
    }



    mod inner {

        use std::io::Error as IoError;
        use std::io::Write;

        use flv_future_core::test_async;
        
        use super::temp_dir;
        use super::ensure_clean_file;

        #[test_async]
        async fn test_sink_file_write_std() -> Result<(), IoError> {

            use std::fs::File;

            const WRITE_FILE: &str = "file_test_two_write_std";

            let test_file = temp_dir().join(WRITE_FILE);
            ensure_clean_file(&test_file);
            let mut f_sink = File::create(&test_file).expect("file created");

            let bytes: Vec<u8> = vec![0x01; 73];
            f_sink.write_all(&bytes).expect("send success");
            let bytes: Vec<u8> = vec![0x01; 74];
            f_sink.write_all(&bytes).expect("send success");
            
            let metadata = std::fs::metadata(test_file).expect("data file should exists");

            // even if file is not flushed, file has all the data 
            assert_eq!(metadata.len(),147);
            
            Ok(())
        }
    }

}
