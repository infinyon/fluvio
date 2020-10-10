use std::io::Error as IoError;
use std::path::PathBuf;
use std::path::Path;

use tracing::debug;
use tracing::trace;
use futures_lite::io::AsyncWriteExt;

use fluvio_future::fs::File;
use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_future::fs::BoundedFileSink;
use fluvio_future::fs::BoundedFileOption;
use fluvio_future::fs::BoundedFileSinkError;
use dataplane::batch::DefaultBatch;
use dataplane::{Offset, Size};
use dataplane::core::Encoder;

use crate::util::generate_file_name;
use crate::validator::validate;
use crate::validator::LogValidationError;
use crate::ConfigOption;
use crate::StorageError;
use crate::records::FileRecords;

pub const MESSAGE_LOG_EXTENSION: &str = "log";

/// Can append new batch to file
pub struct MutFileRecords {
    base_offset: Offset,
    item_last_offset_delta: Size,
    f_sink: BoundedFileSink,
    path: PathBuf,
}

impl Unpin for MutFileRecords {}

impl MutFileRecords {
    pub async fn create(
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<MutFileRecords, BoundedFileSinkError> {
        let sink_option = BoundedFileOption {
            max_len: Some(option.segment_max_bytes as u64),
        };
        let log_path = generate_file_name(&option.base_dir, base_offset, MESSAGE_LOG_EXTENSION);
        debug!("creating log at: {}", log_path.display());
        let f_sink = BoundedFileSink::open_append(&log_path, sink_option).await?;
        Ok(MutFileRecords {
            base_offset,
            f_sink,
            item_last_offset_delta: 0,
            path: log_path.to_owned(),
        })
    }

    pub async fn open(
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<MutFileRecords, StorageError> {
        let log_path = generate_file_name(&option.base_dir, base_offset, MESSAGE_LOG_EXTENSION);
        debug!("opening commit log at: {}", log_path.display());

        let sink_option = BoundedFileOption {
            max_len: Some(option.segment_max_bytes as u64),
        };

        let f_sink = BoundedFileSink::open_append(&log_path, sink_option).await?;
        Ok(MutFileRecords {
            base_offset,
            f_sink,
            item_last_offset_delta: 0,
            path: log_path.to_owned(),
        })
    }

    pub fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    pub async fn validate(&mut self) -> Result<Offset, LogValidationError> {
        validate(self.f_sink.get_path()).await
    }

    pub fn get_pos(&self) -> Size {
        self.f_sink.get_current_len() as Size
    }

    pub fn get_item_last_offset_delta(&self) -> Size {
        self.item_last_offset_delta
    }

    pub async fn send(&mut self, item: DefaultBatch) -> Result<(), StorageError> {
        trace!("start sending using batch {:#?}", item.get_header());
        self.item_last_offset_delta = item.get_last_offset_delta();
        let mut buffer: Vec<u8> = vec![];
        item.encode(&mut buffer, 0)?;
        trace!("start sending finally {} bytes", buffer.len());
        if self.f_sink.can_be_appended(buffer.len() as u64) {
            self.f_sink.write_all(&buffer).await?;
            // for now, we flush for every send
            self.f_sink.flush().await.map_err(|err| err.into())
        } else {
            Err(StorageError::NoRoom(item))
        }
    }

    #[allow(unused)]
    pub async fn flush(&mut self) -> Result<(), IoError> {
        self.f_sink.flush().await
    }
}

impl FileRecords for MutFileRecords {
    fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    fn get_file(&self) -> &File {
        &self.f_sink.inner()
    }

    fn get_path(&self) -> &Path {
        &self.path
    }

    fn as_file_slice(&self, start: Size) -> Result<AsyncFileSlice, IoError> {
        self.f_sink
            .slice_from(start as u64, self.f_sink.get_current_len() - start as u64)
    }

    fn as_file_slice_from_to(&self, start: Size, len: Size) -> Result<AsyncFileSlice, IoError> {
        self.f_sink.slice_from(start as u64, len as u64)
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::io::Cursor;

    use tracing::debug;

    use fluvio_future::test_async;
    use dataplane::batch::DefaultBatch;
    use dataplane::core::Decoder;
    use dataplane::core::Encoder;
    use flv_util::fixture::ensure_clean_file;

    use super::MutFileRecords;
    use super::StorageError;
    use crate::fixture::create_batch;
    use crate::fixture::read_bytes_from_file;
    use crate::ConfigOption;

    const TEST_FILE_NAME: &str = "00000000000000000100.log"; // for offset 100

    #[test_async]
    async fn test_write_records() -> Result<(), StorageError> {
        let test_file = temp_dir().join(TEST_FILE_NAME);
        ensure_clean_file(&test_file);

        let options = ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            ..Default::default()
        };
        let mut msg_sink = MutFileRecords::create(100, &options).await.expect("create");

        let batch = create_batch();
        let write_size = batch.write_size(0);
        debug!("write size: {}", write_size); // for now, this is 79 bytes
        msg_sink.send(create_batch()).await.expect("create");

        let bytes = read_bytes_from_file(&test_file).expect("read bytes");
        assert_eq!(bytes.len(), write_size, "incorrect size for write");

        let batch = DefaultBatch::decode_from(&mut Cursor::new(bytes), 0)?;
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records.len(), 2);
        let mut records = batch.records;
        assert_eq!(records.len(), 2);
        let record1 = records.remove(0);
        assert_eq!(record1.value.inner_value(), Some(vec![10, 20]));
        let record2 = records.remove(0);
        assert_eq!(record2.value.inner_value(), Some(vec![10, 20]));

        msg_sink.send(create_batch()).await?;
        let bytes = read_bytes_from_file(&test_file)?;
        assert_eq!(bytes.len(), write_size * 2, "should be 158 bytes");

        let old_msg_sink = MutFileRecords::open(100, &options).await?;
        assert_eq!(old_msg_sink.get_base_offset(), 100);

        Ok(())
    }
}
