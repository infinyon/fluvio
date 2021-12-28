use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;

use dataplane::batch::BatchRecords;
use tracing::{debug, warn, trace};

use dataplane::Offset;

use crate::batch::FileBatchStream;
use crate::batch::SequentialMmap;
use crate::batch::StorageBytesIterator;
use crate::batch_header::FileEmptyRecords;
use crate::util::log_path_get_offset;
use crate::util::OffsetError;

#[derive(Debug, thiserror::Error)]
pub enum LogValidationError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("Invalid extension")]
    InvalidExtension,
    #[error("Invalid log name")]
    LogName(#[from] OffsetError),
    #[error("Base off error")]
    BaseOff,
    #[error("Offset not ordered")]
    OffsetNotOrdered,
    #[error("No batches")]
    NoBatches,
    #[error("Batch already exists")]
    ExistingBatch,
    #[error("Empty file: {0}")]
    Empty(i64),
}

/// Validation Log file
#[derive(Debug, Default)]
pub struct LogValidatorResult {
    pub base_offset: Offset,
    pub batches: u32,
    pub last_offset: Offset,
    pub success: u32,
    pub failed: u32,
}

impl LogValidatorResult {
    /// open validator on the log file path
    pub async fn validate<P, R, S>(path: P) -> Result<Self, LogValidationError>
    where
        P: AsRef<Path>,
        R: BatchRecords + Default + std::fmt::Debug,
        S: StorageBytesIterator,
    {
        let file_path = path.as_ref();

        let mut val = Self {
            base_offset: log_path_get_offset(file_path)?,
            last_offset: -1,
            ..Default::default()
        };

        debug!(
            file_name = %file_path.display(),
            val.base_offset,
            "validating",
        );

        let mut batch_stream: FileBatchStream<R, S> = match FileBatchStream::open(file_path).await {
            Ok(batch_stream) => batch_stream,
            Err(err) => match err.kind() {
                ErrorKind::UnexpectedEof => return Err(LogValidationError::Empty(val.base_offset)),
                _ => return Err(err.into()),
            },
        };

        while let Some(batch_pos) = batch_stream.next().await {
            let batch_base_offset = batch_pos.get_batch().get_base_offset();
            let header = batch_pos.get_batch().get_header();
            let offset_delta = header.last_offset_delta;

            trace!(batch_base_offset, offset_delta, "found batch");

            if batch_base_offset < val.base_offset {
                warn!(
                    "batch base offset: {} is less than base offset: {} path: {:#?}",
                    batch_base_offset,
                    val.base_offset,
                    file_path.display()
                );
                return Err(LogValidationError::BaseOff);
            }

            /*
            // test converting batch to slice

            for record in batch_pos.get_batch().records() {

                let result: Result<serde_json::Value,_> = serde_json::from_slice(record.value.as_ref());
                match result {
                    Ok(_) => val.success += 1,
                    Err(err) => {
                        val.failed += 1;
                        if val.failed < 2 {
                            println!("{}",record.value.as_str().unwrap());
                            println!("failed: {:#?}",err);
                        }
                    }
                }
            }
            */

            val.last_offset = batch_base_offset + offset_delta as Offset;

            // perform a simple json decoding

            val.batches += 1;
        }

        if let Some(err) = batch_stream.invalid() {
            return Err(err.into());
        }

        debug!(val.last_offset, "found last offset");
        Ok(val)
    }

    fn next_offset(&self) -> Offset {
        if self.last_offset == -1 {
            return self.base_offset;
        }

        self.last_offset + 1
    }
}

/// validate the file and find last offset
/// if file is not valid then return error
pub async fn validate<P>(path: P) -> Result<Offset, LogValidationError>
where
    P: AsRef<Path>,
{
    match LogValidatorResult::validate::<_, FileEmptyRecords, SequentialMmap>(path).await {
        Ok(val) => Ok(val.next_offset()),
        Err(LogValidationError::Empty(base_offset)) => Ok(base_offset),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::io::ErrorKind;

    use flv_util::fixture::ensure_new_dir;
    use futures_lite::io::AsyncWriteExt;

    use fluvio_future::fs::BoundedFileSink;
    use fluvio_future::fs::BoundedFileOption;
    use dataplane::Offset;

    use crate::fixture::BatchProducer;
    use crate::mut_records::MutFileRecords;
    use crate::config::ReplicaConfig;
    use crate::records::FileRecords;
    use crate::validator::LogValidationError;

    use super::validate;

    #[fluvio_future::test]
    async fn test_validate_empty() {
        const BASE_OFFSET: Offset = 301;

        let test_dir = temp_dir().join("validation_empty");
        ensure_new_dir(&test_dir).expect("new");

        let options = ReplicaConfig {
            base_dir: test_dir,
            segment_max_bytes: 1000,
            ..Default::default()
        }
        .shared();

        let log_records = MutFileRecords::create(BASE_OFFSET, options)
            .await
            .expect("open");
        let log_path = log_records.get_path().to_owned();
        drop(log_records);

        let next_offset = validate(&log_path).await.expect("validate");
        assert_eq!(next_offset, BASE_OFFSET);
    }

    #[fluvio_future::test]
    async fn test_validate_success() {
        const BASE_OFFSET: Offset = 601;

        let test_dir = temp_dir().join("validation_success");
        ensure_new_dir(&test_dir).expect("new");

        let options = ReplicaConfig {
            base_dir: test_dir,
            segment_max_bytes: 1000,
            ..Default::default()
        }
        .shared();

        let mut msg_sink = MutFileRecords::create(BASE_OFFSET, options)
            .await
            .expect("create");

        let mut builder = BatchProducer::builder()
            .base_offset(BASE_OFFSET)
            .build()
            .expect("build");

        msg_sink.write_batch(&builder.batch()).await.expect("write");
        msg_sink
            .write_batch(&builder.batch_records(3))
            .await
            .expect("write");

        let log_path = msg_sink.get_path().to_owned();
        drop(msg_sink);

        let next_offset = validate(&log_path).await.expect("validate");
        assert_eq!(next_offset, BASE_OFFSET + 5);
    }

    #[fluvio_future::test]
    async fn test_validate_invalid_contents() {
        const OFFSET: i64 = 501;

        let test_dir = temp_dir().join("validate_invalid_contents");
        ensure_new_dir(&test_dir).expect("new");

        let options = ReplicaConfig {
            base_dir: test_dir,
            segment_max_bytes: 1000,
            ..Default::default()
        }
        .shared();

        let mut msg_sink = MutFileRecords::create(OFFSET, options)
            .await
            .expect("record created");

        let mut builder = BatchProducer::builder()
            .base_offset(OFFSET)
            .build()
            .expect("build");

        msg_sink
            .write_batch(&builder.batch())
            .await
            .expect("create batch");

        let test_file = msg_sink.get_path().to_owned();

        // add some junk
        let mut f_sink = BoundedFileSink::create(&test_file, BoundedFileOption::default())
            .await
            .expect("open batch file");
        let bytes = vec![0x01, 0x02, 0x03];
        f_sink.write_all(&bytes).await.expect("write some junk");
        f_sink.flush().await.expect("flush");
        match validate(&test_file).await {
            Err(err) => match err {
                LogValidationError::Io(io_err) => {
                    assert!(matches!(io_err.kind(), ErrorKind::UnexpectedEof));
                }
                _ => panic!("unexpected error"),
            },
            Ok(_) => panic!("should have failed"),
        };
    }
}

#[cfg(test)]
mod perf {

    use std::time::Instant;

    use dataplane::batch::MemoryRecords;

    use crate::batch_header::FileEmptyRecords;

    use super::*;

    //#[fluvio_future::test]
    #[allow(unused)]
    // execute this with: `cargo test perf_test -p fluvio-storage --release -- --nocapture`
    async fn perf_test() {
        const TEST_PATH: &str = "/tmp/perf/00000000000000000000.log";

        println!("starting test");
        let header_time = Instant::now();
        let msm_result =
            LogValidatorResult::validate::<_, FileEmptyRecords, SequentialMmap>(TEST_PATH)
                .await
                .expect("validate");
        println!("header only took: {:#?}", header_time.elapsed());
        println!("validator: {:#?}", msm_result);

        let record_time = Instant::now();
        let _ = LogValidatorResult::validate::<_, MemoryRecords, SequentialMmap>(TEST_PATH)
            .await
            .expect("validate");
        println!("full scan took: {:#?}", record_time.elapsed());
    }
}
