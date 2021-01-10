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

/// MutFileRecordsFlushPolicy describes and implements a flush policy
pub enum MutFileRecordsFlushPolicy {
    NoFlush,
    EveryWrite,
    CountWrites { n_writes: u32, write_tracking: u32 },
}

/// Can append new batch to file
pub struct MutFileRecords {
    base_offset: Offset,
    item_last_offset_delta: Size,
    f_sink: BoundedFileSink,
    flush_policy: MutFileRecordsFlushPolicy,
    write_count: u64,
    path: PathBuf,
}

impl Unpin for MutFileRecords {}

fn get_flush_policy_from_config(option: &ConfigOption) -> MutFileRecordsFlushPolicy {
    if option.flush_write_count == 0 {
        MutFileRecordsFlushPolicy::NoFlush
    } else if option.flush_write_count == 1 {
        MutFileRecordsFlushPolicy::EveryWrite
    } else {
        MutFileRecordsFlushPolicy::CountWrites {
            n_writes: option.flush_write_count,
            write_tracking: 0,
        }
    }
}

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
            flush_policy: get_flush_policy_from_config(option),
            write_count: 0,
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
            flush_policy: get_flush_policy_from_config(option),
            write_count: 0,
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

        if self.f_sink.can_be_appended(buffer.len() as u64) {
            debug!("writing {} bytes at: {}", buffer.len(), self.path.display());
            self.f_sink.write_all(&buffer).await?;
            self.write_count = self.write_count.saturating_add(1);
            if self.flush_policy.should_flush() {
                self.f_sink.flush().await?;
            }
            Ok(())
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

impl MutFileRecordsFlushPolicy {
    /// Evaluates the flush policy and returns true
    /// if the policy determines the need to flush
    fn should_flush(&mut self) -> bool {
        use MutFileRecordsFlushPolicy::*;

        match self {
            NoFlush => false,

            EveryWrite => true,

            CountWrites {
                n_writes: n_max,
                write_tracking: wcount,
            } => {
                *wcount += 1;
                if *wcount >= *n_max {
                    *wcount = 0;
                    return true;
                }
                false
            }
        }
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
    const TEST_FILE_NAMEC: &str = "00000000000000000200.log"; // for offset 200

    #[test_async]
    async fn test_write_records_every() -> Result<(), StorageError> {
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

    // This Test configures policy to flush after every NUM_WRITES and ensure
    // the data is still durably retained on a postive test of triggering the
    // flush. A negative test of the flush being held off was performed by
    // inspection, and is not automated because of variable results. When a
    // fs/storage system does committed internal writes is a race between the
    // system and an application flushing above.
    #[test_async]
    async fn test_write_records_count() -> Result<(), StorageError> {
        let test_file = temp_dir().join(TEST_FILE_NAMEC);
        ensure_clean_file(&test_file);

        const NUM_WRITES: u32 = 4;
        const OFFSET: i64 = 200;

        let options = ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            flush_write_count: NUM_WRITES,
            ..Default::default()
        };
        let mut msg_sink = MutFileRecords::create(OFFSET, &options)
            .await
            .expect("create");

        let batch = create_batch();
        let write_size = batch.write_size(0);
        debug!("write size: {}", write_size); // for now, this is 79 bytes
        msg_sink.send(create_batch()).await.expect("create");
        msg_sink.flush().await.expect("create flush"); // ensure the file is created

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

        for _ in 1..NUM_WRITES {
            msg_sink.send(create_batch()).await.expect("send");
        }

        let bytes = read_bytes_from_file(&test_file).expect("read bytes final");
        let nbytes = write_size * NUM_WRITES as usize;
        assert_eq!(bytes.len(), nbytes, "should be {} bytes", nbytes);

        let old_msg_sink = MutFileRecords::open(OFFSET, &options)
            .await
            .expect("check old sink");
        assert_eq!(old_msg_sink.get_base_offset(), OFFSET);

        Ok(())
    }
}
