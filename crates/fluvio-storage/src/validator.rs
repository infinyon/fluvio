use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use fluvio_protocol::record::BatchRecords;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::trace;
use tracing::{debug};
use anyhow::{Result, anyhow};

use fluvio_protocol::record::Offset;

use crate::batch::BatchHeaderError;
use crate::batch::FileBatchStream;
use crate::batch::StorageBytesIterator;
use crate::batch_header::FileEmptyRecords;
use crate::file::FileBytesIterator;
use crate::index::Index;
use crate::util::log_path_get_offset;

#[derive(Debug, thiserror::Error)]
pub enum LogValidationError {
    #[error("Invalid extension")]
    InvalidExtension,
    #[error("Batch decoding error: {0}")]
    BatchDecoding(#[from] BatchHeaderError),
    #[error("batch offset is less than base offset: {invalid_batch_offset}")]
    InvalidBaseOffsetMinimum { invalid_batch_offset: Offset },
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid Index: {offset} pos: {batch_file_pos} offset: {index_position}")]
pub struct InvalidIndexError {
    offset: Offset,
    batch_file_pos: u32,
    index_position: u32,
    diff_position: u32,
}

/// Validation Log file is consistent with index file
#[derive(Debug, Default)]
pub struct LogValidator {
    pub base_offset: Offset,
    file_path: PathBuf,
    pub batches: u32,
    last_valid_offset: Offset,
    pub last_valid_batch_pos: u32, // starting position of last successful batch
    pub last_valid_file_pos: u32,  // end position of last successful batch
    pub duration: Duration,
    pub error: Option<LogValidationError>,
    pub index_error: Option<InvalidIndexError>,
}

impl LogValidator {
    async fn validate_core<I, S, R>(path: impl AsRef<Path>, index: Option<&I>) -> Result<Self>
    where
        I: Index,
        S: StorageBytesIterator,
        R: BatchRecords + Default + std::fmt::Debug,
    {
        let file_path = path.as_ref().to_path_buf();
        let mut val = Self {
            base_offset: log_path_get_offset(&file_path)?,
            last_valid_offset: -1,
            file_path,
            ..Default::default()
        };

        info!(
            file_name = %val.file_path.display(),
            val.base_offset,
            "validating log segment",
        );

        let start_time = std::time::Instant::now();
        let batch_stream: FileBatchStream<R, S> = match FileBatchStream::open(&val.file_path).await
        {
            Ok(batch_stream) => batch_stream,
            Err(err) => match err.kind() {
                ErrorKind::UnexpectedEof => {
                    return Err(anyhow!("empty file with base offset: {}", val.base_offset))
                }
                _ => return Err(err.into()),
            },
        };

        // find recoverable error

        if let Err(err) = val.validate_with_stream(batch_stream, index).await {
            error!(%err, "found error from stream");
            error!("{:#?} debug", err);
            val.duration = start_time.elapsed();
            match err.downcast_ref::<BatchHeaderError>() {
                Some(header_error) => {
                    error!(%header_error, "found batch header error, turn into recoverable error");
                    val.error = Some(LogValidationError::BatchDecoding(header_error.clone()));
                    Ok(val)
                }
                _ => Err(err),
            }
        } else {
            val.duration = start_time.elapsed();
            Ok(val)
        }
    }

    /// validate log file
    #[instrument(skip(self, index, batch_stream))]
    async fn validate_with_stream<I, R, S>(
        &mut self,
        mut batch_stream: FileBatchStream<R, S>,
        index: Option<&I>,
    ) -> Result<()>
    where
        I: Index,
        S: StorageBytesIterator,
        R: BatchRecords + Default + std::fmt::Debug,
    {
        let mut last_index_pos = 0;

        while let Some(batch_pos) = batch_stream.try_next().await? {
            let current_batch_pos = batch_pos.get_pos();
            let current_batch = batch_pos.inner();

            let current_batch_offset = current_batch.get_base_offset();

            let header = current_batch.get_header();
            let offset_delta = header.last_offset_delta;

            // if current batch offset is less than base offset, it's invalid, we abort
            if current_batch_offset < self.base_offset {
                error!(
                    last_valid_offset = self.last_valid_offset,
                    last_valid_pos = self.last_valid_batch_pos,
                    current_batch_offset,
                    base_offset = self.base_offset,
                    "found offset less than base offset,aborting"
                );
                self.error = Some(LogValidationError::InvalidBaseOffsetMinimum {
                    invalid_batch_offset: current_batch_offset,
                });
                return Ok(());
            }

            // set high watermark for validating batches
            self.last_valid_offset = current_batch_offset + offset_delta as Offset;
            self.last_valid_batch_pos = current_batch_pos;
            self.last_valid_file_pos = batch_stream.get_pos();
            self.batches += 1;

            // self.last_valid_file_pos = file

            // perform index validation
            if self.index_error.is_none() {
                trace!(
                    diff_pos = current_batch_pos - self.last_valid_batch_pos,
                    "found batch offset"
                );

                // offset relative to segment
                let delta_offset = current_batch_offset - self.base_offset;

                if let Some(index) = index {
                    if let Some((offset, index_pos)) = index.find_offset(delta_offset as u32) {
                        if offset == delta_offset as u32 {
                            trace!(diff_pos = index_pos - last_index_pos, "found index offset");

                            if index_pos != current_batch_pos {
                                error!(
                                    diff_index_batch_pos = index_pos - current_batch_pos,
                                    diff_index_pos = index_pos - last_index_pos,
                                    "index mismatch"
                                );

                                // found index error
                                self.index_error = Some(InvalidIndexError {
                                    offset: current_batch_offset,
                                    batch_file_pos: current_batch_pos,
                                    index_position: index_pos,
                                    diff_position: index_pos - current_batch_pos,
                                });
                            } else {
                                last_index_pos = index_pos;
                            }
                        } else {
                            /*
                            trace!(
                                delta_offset,
                                offset,
                                index_pos,
                                "- different offset from index, skipping"
                            );
                            */
                        }
                    } else {
                        //  trace!(delta_offset, "no index found");
                    }
                }
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
        }

        debug!(
            self.last_valid_offset,
            "validation completed, found last offset"
        );

        Ok(())
    }

    /// return leo
    pub(crate) fn leo(&self) -> Offset {
        if self.last_valid_offset == -1 {
            return self.base_offset;
        }

        self.last_valid_offset + 1
    }

    /// public facing entry point
    #[instrument(skip(index, path))]
    pub(crate) async fn validate<I, S>(path: impl AsRef<Path>, index: Option<&I>) -> Result<Self>
    where
        I: Index,
        S: StorageBytesIterator,
    {
        Self::validate_core::<I, S, FileEmptyRecords>(path, index).await
    }

    #[instrument(skip(index, path))]
    pub(crate) async fn default_validate<I>(
        path: impl AsRef<Path>,
        index: Option<&I>,
    ) -> Result<Self>
    where
        I: Index,
    {
        Self::validate::<I, FileBytesIterator>(path, index).await
    }
}

/// validate the file and find last offset
/// if file is not valid then return error

#[cfg(test)]
mod tests {

    use std::env::temp_dir;

    use flv_util::fixture::ensure_new_dir;
    use futures_lite::io::AsyncWriteExt;

    use fluvio_protocol::record::Offset;

    use crate::LogIndex;
    use crate::fixture::BatchProducer;
    use crate::mut_records::MutFileRecords;
    use crate::config::ReplicaConfig;
    use crate::records::FileRecords;

    use super::*;

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

        let validator = LogValidator::default_validate::<LogIndex>(&log_path, None)
            .await
            .expect("validate");
        assert_eq!(validator.leo(), BASE_OFFSET);
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

        // at this point, we have written 5 records

        let log_path = msg_sink.get_path().to_owned();
        drop(msg_sink);

        let original_fs_len = std::fs::metadata(&log_path).expect("get metadata").len();

        let validator = LogValidator::default_validate::<LogIndex>(&log_path, None)
            .await
            .expect("validate");
        assert_eq!(validator.leo(), BASE_OFFSET + 5);
        assert_eq!(validator.last_valid_file_pos as u64, original_fs_len);
        assert_eq!(validator.batches, 2);
    }

    #[fluvio_future::test]
    async fn test_validating_invalid_contents() {
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

        // write a batch with 3 records
        // each batch is 88 bytes long
        msg_sink
            .write_batch(&builder.batch_records(3))
            .await
            .expect("write");

        // write a batch with 3 records
        msg_sink
            .write_batch(&builder.batch_records(3))
            .await
            .expect("write");

        msg_sink.flush().await.expect("flush");
        let test_fs_path = msg_sink.get_path().to_owned();
        drop(msg_sink);

        let validator = LogValidator::default_validate::<LogIndex>(&test_fs_path, None)
            .await
            .expect("validate");

        // we have written 3 records, leo = 3 + 1
        assert_eq!(validator.leo(), OFFSET + 6);
        drop(validator);

        // get size of the file
        let original_fs_len = std::fs::metadata(&test_fs_path)
            .expect("get metadata")
            .len();

        // append some junk
        let mut file = fluvio_future::fs::util::open_read_append(&test_fs_path)
            .await
            .expect("opening log file");
        let bytes = vec![0x01, 0x02, 0x03];
        file.write_all(&bytes).await.expect("write some junk");
        file.flush().await.expect("flush");
        drop(file);

        let invalid_fs_len = std::fs::metadata(&test_fs_path)
            .expect("get metadata")
            .len();
        assert_eq!(invalid_fs_len, original_fs_len + 3);

        debug!("checking invalid contents");
        let validator = LogValidator::default_validate::<LogIndex>(&test_fs_path, None)
            .await
            .expect("validate");

        // stats for validated offets should be original contents
        assert_eq!(validator.leo(), OFFSET + 6); // should have same leo as successfull validation
        assert_eq!(validator.last_valid_file_pos as u64, original_fs_len);
        assert_eq!(validator.batches, 2);
        let err = validator.error.expect("error");
        assert!(matches!(err, LogValidationError::BatchDecoding(_)));
    }
}

#[cfg(test)]
mod perf {

    use std::time::Instant;

    use crate::{LogIndex};

    use super::*;

    //#[fluvio_future::test]
    #[allow(unused)]
    // execute this with: `cargo test perf_test -p fluvio-storage --release -- --nocapture`
    async fn perf_test() {
        const TEST_PATH: &str = "/tmp/perf/00000000000000000000.log";

        println!("starting test");
        let header_time = Instant::now();
        let msm_result = LogValidator::default_validate::<LogIndex>(TEST_PATH, None)
            .await
            .expect("validate");
        println!("header only took: {:#?}", header_time.elapsed());
        println!("validator: {msm_result:#?}");

        /*
        let record_time = Instant::now();
        let _ = LogValidator::<_, MemoryRecords, LogIndex>::validate_segment(TEST_PATH, None, false, false)
            .await
            .expect("validate");
        println!("full scan took: {:#?}", record_time.elapsed());
        */
    }
}
