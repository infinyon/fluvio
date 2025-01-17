use std::io::Cursor;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use bytes::Buf;
use fluvio_protocol::record::Offset;
use fluvio_types::event::offsets::OffsetChangeListener;
use fluvio_types::event::offsets::OffsetPublisher;
use fluvio_types::event::StickyEvent;
use futures_lite::io::AsyncReadExt;
use futures_lite::io::AsyncWriteExt;
use futures_lite::io::AsyncSeekExt;
use tokio::select;
use tracing::debug;
use tracing::error;
use tracing::info;

use fluvio_future::fs::File;
use fluvio_future::fs::metadata;
use fluvio_future::fs::util;

use crate::config::SharedReplicaConfig;

pub const HW_CHECKPOINT_FILE_NAME: &str = "replication.chk";

pub trait ReadToBuf: Sized {
    fn read_from<B>(buf: &mut B) -> Self
    where
        B: Buf;
}

impl ReadToBuf for i64 {
    fn read_from<B>(buf: &mut B) -> Self
    where
        B: Buf,
    {
        buf.get_i64()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct CheckPoint {
    option: Arc<SharedReplicaConfig>,
    offset: Arc<OffsetPublisher>,
    flush: Arc<StickyEvent>,
    path: PathBuf,
}

impl CheckPoint {
    pub async fn create(
        option: Arc<SharedReplicaConfig>,
        name: &str,
        initial_offset: Offset,
    ) -> Result<Self, IoError> {
        let checkpoint_path = option.base_dir.join(name);

        match metadata(&checkpoint_path).await {
            Ok(_) => {
                info!("checkpoint {:#?} exists, reading", checkpoint_path);
                let file = util::open_read_write(&checkpoint_path).await?;
                let mut lazy_writer = LazyWriter::new(file);
                let old_offset = match lazy_writer.read().await {
                    Ok(offset) => {
                        info!(offset = %offset,"checkpoint old offset read");
                        offset
                    }
                    Err(err) => {
                        error!("error reading checkpoint: {}, resetting", err);
                        0
                    }
                };
                let offset = OffsetPublisher::shared(old_offset);
                let flush = lazy_writer.spawn_writer(offset.change_listener());
                Ok(CheckPoint {
                    option: option.clone(),
                    path: checkpoint_path,
                    offset,
                    flush,
                })
            }
            Err(_) => {
                info!(
                    "no existing creating checkpoint {}, creating",
                    checkpoint_path.display()
                );
                let file = util::open_read_write(&checkpoint_path).await?;
                info!("checkfile created: {:#?}", checkpoint_path);
                let lazy_writer = LazyWriter::new(file);
                let offset = OffsetPublisher::shared(initial_offset);
                let flush = lazy_writer.spawn_writer(offset.change_listener());
                Ok(CheckPoint {
                    option: option.clone(),
                    path: checkpoint_path,
                    flush,
                    offset,
                })
            }
        }
    }

    pub fn get_offset(&self) -> Offset {
        self.offset.current_value()
    }

    /// return last modified time
    pub async fn get_last_modified(&self) -> Result<std::time::SystemTime, IoError> {
        let file = util::open_read_write(&self.path).await?;
        let metadata = file.metadata().await?;
        metadata.modified()
    }

    pub fn write(&mut self, pos: Offset) {
        debug!(%pos,"Update checkpoint");
        self.offset.update(pos);
    }
}

impl Drop for CheckPoint {
    fn drop(&mut self) {
        info!("dropping checkpoint");
        self.flush.notify();
    }
}

/// write in lazy
#[derive(Debug)]
struct LazyWriter {
    file: File,
    offset: Offset,
}

impl LazyWriter {
    fn new(file: File) -> Self {
        Self { file, offset: 0 }
    }

    fn spawn_writer(self, listener: OffsetChangeListener) -> Arc<StickyEvent> {
        let flush_event = StickyEvent::shared();
        let loop_flush = flush_event.clone();
        fluvio_future::task::spawn(async move {
            if let Err(err) = self.write_loop(listener, loop_flush).await {
                error!("error writing checkpoint: {}", err);
            }
        });
        flush_event
    }

    async fn write_loop(
        mut self,
        mut listener: OffsetChangeListener,
        flush: Arc<StickyEvent>,
    ) -> Result<()> {
        info!("starting checkpoint writer loop");
        loop {
            select! {
                _ = flush.listen() => {
                    info!("flushing checkpoint");
                    self.flush().await?;
                    break;
                },
                offset = listener.listen() => {
                    if offset == self.offset {
                        debug!(offset, "no change in offset, skipping");
                    } else {
                        debug!(offset, "writing checkpoint");
                        let buf = offset.to_be_bytes();
                        self.file.seek(SeekFrom::Start(0)).await?;
                        self.file.write_all(&buf).await?;
                    }
                }
            }
        }

        info!("lazy writer loop done");
        Ok(())
    }

    /// get offset from file
    async fn read(&mut self) -> Result<Offset, IoError> {
        self.file.seek(SeekFrom::Start(0)).await?;
        let mut contents = Vec::new();
        self.file.read_to_end(&mut contents).await?;

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
        info!(offset = self.offset, "checkpoint read");
        Ok(self.offset)
    }

    async fn flush(&mut self) -> Result<(), IoError> {
        info!("lazywriter flushing checkpoint");
        self.file.sync_all().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;

    use fluvio_future::timer::sleep;
    use flv_util::fixture::ensure_clean_file;

    use crate::config::ReplicaConfig;
    use super::CheckPoint;

    use super::*;

    #[fluvio_future::test]
    async fn lister_test() {
        let publisher = OffsetPublisher::shared(10);
        let mut listener = publisher.change_listener();
        assert!(listener.listen().await == 10);

        publisher.update(21);
        assert!(listener.listen().await == 21);

        publisher.update(30);
        publisher.update(40);
        assert!(listener.listen().await == 40);
    }

    #[fluvio_future::test]
    async fn checkpoint_test() {
        let test_file = temp_dir().join("test.chk");
        ensure_clean_file(&test_file);

        let option = ReplicaConfig {
            base_dir: temp_dir(),
            ..Default::default()
        }
        .shared();

        let mut ck = CheckPoint::create(option.clone(), "test.chk", 0)
            .await
            .expect("create");
        assert_eq!(ck.get_offset(), 0);
        ck.write(10);
        ck.write(40);

        // allow time for checkpoint to write
        sleep(std::time::Duration::from_millis(1000)).await;
        drop(ck);

        // wait until checkpoint is fully fully flushed
        sleep(std::time::Duration::from_millis(1000)).await;

        let ck2 = CheckPoint::create(option, "test.chk", 0)
            .await
            .expect("restore");

        assert_eq!(ck2.get_offset(), 40);
    }
}
