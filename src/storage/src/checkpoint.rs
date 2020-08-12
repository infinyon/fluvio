use std::fmt::Display;
use std::io::Cursor;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::SeekFrom;

use bytes::Buf;
use bytes::BufMut;
use futures::io::AsyncReadExt;
use futures::io::AsyncWriteExt;
use futures::io::AsyncSeekExt;
use tracing::debug;
use tracing::trace;

use flv_future_aio::fs::File;
use flv_future_aio::fs::metadata;
use flv_future_aio::fs::util;

use crate::ConfigOption;

pub trait ReadToBuf: Sized {
    fn read_from<B>(buf: &mut B) -> Self
    where
        B: Buf;

    fn write_to<B>(&mut self, buf: &mut B)
    where
        B: BufMut;
}

impl ReadToBuf for u64 {
    fn read_from<B>(buf: &mut B) -> Self
    where
        B: Buf,
    {
        buf.get_u64()
    }

    fn write_to<B>(&mut self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put_u64(*self);
    }
}

impl ReadToBuf for i64 {
    fn read_from<B>(buf: &mut B) -> Self
    where
        B: Buf,
    {
        buf.get_i64()
    }

    fn write_to<B>(&mut self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put_i64(*self);
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct CheckPoint<T> {
    option: ConfigOption,
    offset: T,
    file: File,
}

impl<T> CheckPoint<T>
where
    T: Display + ReadToBuf + Clone + Sized + 'static,
{
    pub async fn create(
        option: &ConfigOption,
        name: &str,
        initial_offset: T,
    ) -> Result<Self, IoError> {
        let checkpoint_path = option.base_dir.join(name);

        match metadata(&checkpoint_path).await {
            Ok(_) => {
                trace!("checkpoint {:#?} exists, reading", checkpoint_path);
                let file = util::open_read_write(&checkpoint_path).await?;
                let mut checkpoint = CheckPoint {
                    option: option.to_owned(),
                    file,
                    offset: initial_offset.clone(),
                };
                checkpoint.read().await?;
                Ok(checkpoint)
            }
            Err(_) => {
                debug!(
                    "no existing creating checkpoint {:#?}, creating",
                    checkpoint_path
                );
                let file = util::open_read_write(&checkpoint_path).await?;
                trace!("file created: {:#?}", checkpoint_path);
                let mut checkpoint = CheckPoint {
                    option: option.to_owned(),
                    file,
                    offset: initial_offset.clone(),
                };
                checkpoint.write(initial_offset.clone()).await?;
                Ok(checkpoint)
            }
        }
    }

    pub fn get_offset(&self) -> &T {
        &self.offset
    }

    /// read contents of the
    async fn read(&mut self) -> Result<(), IoError> {
        self.file.seek(SeekFrom::Start(0)).await?;
        let mut contents = Vec::new();
        self.file
            .read_to_end(&mut contents)
            .await
            .expect("reading to end");

        if contents.len() != 8 {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "there should be exact 8 bytes but {} bytes available ",
                    contents.len()
                ),
            ));
        }

        let mut buf = Cursor::new(contents);
        self.offset = ReadToBuf::read_from(&mut buf);
        Ok(())
    }

    pub(crate) async fn write(&mut self, pos: T) -> Result<(), IoError> {
        debug!("writing checkpoint: {}", pos);
        self.file.seek(SeekFrom::Start(0)).await?;
        let mut contents = Vec::new();
        self.offset = pos;
        self.offset.write_to(&mut contents);
        self.file.write_all(&contents).await?;
        self.file.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::io::Error as IoError;

    use flv_future_aio::test_async;
    use flv_util::fixture::ensure_clean_file;

    use crate::ConfigOption;
    use super::CheckPoint;

    #[test_async]
    async fn checkpoint_test() -> Result<(), IoError> {
        let test_file = temp_dir().join("test.chk");
        ensure_clean_file(&test_file);

        let option = ConfigOption {
            base_dir: temp_dir(),
            ..Default::default()
        };
        let mut ck: CheckPoint<u64> = CheckPoint::create(&option, "test.chk", 0)
            .await
            .expect("create");
        let _ = ck.read().await.expect("do initial read");
        assert_eq!(*ck.get_offset(), 0);
        ck.write(10).await.expect("first write");
        ck.write(40).await.expect("2nd write");

        drop(ck);

        let mut ck2: CheckPoint<u64> = CheckPoint::create(&option, "test.chk", 0)
            .await
            .expect("restore");
        ck2.read().await?;
        assert_eq!(*ck2.get_offset(), 40);
        ck2.write(20)
            .await
            .expect("write aft er reading should work");
        Ok(())
    }
}
