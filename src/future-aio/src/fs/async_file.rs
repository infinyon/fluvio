use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::SeekFrom;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use log::trace;
use async_trait::async_trait;
use async_std::fs::File;

use async_std::io::prelude::SeekExt;

use super::AsyncFileSlice;

pub mod file_util {

    use std::io::Error as IoError;
    use std::path::Path;

    use async_std::fs::File;
    use async_std::fs::OpenOptions;
    /// open for write only
    pub async fn create<P>(path: P) -> Result<File, IoError>
    where
        P: AsRef<Path>,
    {
        File::create(path.as_ref()).await
    }

    /// open for only read
    pub async fn open<P>(path: P) -> Result<File, IoError>
    where
        P: AsRef<Path>,
    {
        let file_path = path.as_ref();
        File::open(file_path).await
    }

    /// open for read and write
    pub async fn open_read_write<P>(path: P) -> Result<File, IoError>
    where
        P: AsRef<Path>,
    {
        let file_path = path.as_ref();
        let mut option = OpenOptions::new();
        option.read(true).write(true).create(true).append(false);

        option.open(file_path).await
    }

    pub async fn open_read_append<P>(path: P) -> Result<File, IoError>
    where
        P: AsRef<Path>,
    {
        let file_path = path.as_ref();
        let mut option = OpenOptions::new();
        option.read(true).create(true).append(true);

        option.open(file_path).await
    }
}

#[async_trait]
pub trait AsyncFile {
    async fn reset_to_beginning(&mut self) -> Result<(), IoError>;

    fn raw_slice(&self, position: u64, len: u64) -> AsyncFileSlice;

    async fn as_slice(
        &self,
        position: u64,
        desired_len_opt: Option<u64>,
    ) -> Result<AsyncFileSlice, IoError>;
}

#[async_trait]
impl AsyncFile for File {
    async fn reset_to_beginning(&mut self) -> Result<(), IoError> {
        self.seek(SeekFrom::Start(0)).await.map(|_| ())
    }

    /// return raw slice with fiel descriptor, this doesn't not check
    #[cfg(unix)]
    fn raw_slice(&self, position: u64, len: u64) -> AsyncFileSlice {
        AsyncFileSlice::new(self.as_raw_fd(), position, len)
    }

    /// Extract slice of file using file descriptor
    /// if desired len is not supplied, compute the len from metadata
    #[cfg(unix)]
    async fn as_slice(
        &self,
        position: u64,
        desired_len_opt: Option<u64>,
    ) -> Result<AsyncFileSlice, IoError> {
        let metadata = self.metadata().await?;
        let len = metadata.len();
        if position >= len {
            return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                "position is greater than available len",
            ));
        }
        let slice_len = if let Some(desired_len) = desired_len_opt {
            if position + desired_len >= len {
                return Err(IoError::new(
                    ErrorKind::UnexpectedEof,
                    "not available bytes",
                ));
            }
            desired_len
        } else {
            len - position
        };

        trace!("file trace: position: {}, len: {}", position, len);

        Ok(self.raw_slice(position, slice_len))
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::fs::File;
    use std::io::Error as IoError;
    use std::io::Write;
    use std::io::Seek as _;
    use std::io::SeekFrom;
    use std::io::Read;
    use futures::io::AsyncReadExt;
    use futures::io::AsyncWriteExt;
    use async_std::io::prelude::SeekExt;
    use flv_future_core::test_async;
    use utils::fixture::ensure_clean_file;
    use super::AsyncFile;
    use super::file_util;

    // sync seek write and read
    // this is used for implementating async version
    #[test]
    fn test_sync_seek_write() -> Result<(), std::io::Error> {
        let mut option = std::fs::OpenOptions::new();
        option.read(true).write(true).create(true).append(false);
        let mut file = option.open("/tmp/x1")?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(b"test")?;
        //  file.write_all(b"kkk")?;
        file.sync_all()?;

        let mut f2 = File::open("/tmp/x1")?;
        let mut contents = String::new();
        f2.read_to_string(&mut contents)?;
        assert_eq!(contents, "test");
        Ok(())
    }

    #[test_async]
    async fn file_multiple_overwrite() -> Result<(), IoError> {
        let test_file_path = temp_dir().join("file_write_test");
        ensure_clean_file(&test_file_path);

        // write 4 byte string
        let mut file = file_util::create(&test_file_path).await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(b"test").await?;
        file.sync_all().await?;

        // go back beginning and overwrite
        let mut file = file_util::create(&test_file_path).await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(b"xyzt").await?;
        file.sync_all().await?;
        let mut output = Vec::new();
        let mut file = file_util::open(&test_file_path).await?;
        file.read_to_end(&mut output).await?;
        assert_eq!(output.len(), 4);
        let contents = String::from_utf8(output).expect("conversion");
        assert_eq!(contents, "xyzt");

        Ok(())
    }

    #[test_async]
    async fn async_file_write_read_same() -> Result<(), IoError> {
        let test_file_path = temp_dir().join("read_write_test");
        ensure_clean_file(&test_file_path);

        let mut output = Vec::new();
        let mut file = file_util::open_read_write(&test_file_path).await?;
        file.write_all(b"test").await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.read_to_end(&mut output).await?;
        assert_eq!(output.len(), 4);
        let contents = String::from_utf8(output).expect("conversion");
        assert_eq!(contents, "test");

        Ok(())
    }

    #[test_async]
    async fn async_file_write_append_same() -> Result<(), IoError> {
        let test_file_path = temp_dir().join("read_append_test");
        ensure_clean_file(&test_file_path);

        let mut output = Vec::new();
        let mut file = file_util::open_read_append(&test_file_path).await?;
        file.write_all(b"test").await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(b"xyz").await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.read_to_end(&mut output).await?;
        assert_eq!(output.len(), 7);
        let contents = String::from_utf8(output).expect("conversion");
        assert_eq!(contents, "testxyz");

        Ok(())
    }

    #[test_async]
    async fn test_as_slice() -> Result<(), IoError> {
        let file = file_util::open("test-data/apirequest.bin").await?;
        let f_slice = file.as_slice(0, None).await?;
        assert_eq!(f_slice.position(), 0);
        assert_eq!(f_slice.len(), 30);
        Ok(())
    }
}
