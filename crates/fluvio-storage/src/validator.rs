use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use fluvio_protocol::record::BatchRecords;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::{debug};
use anyhow::{Result, anyhow};

use fluvio_protocol::record::Offset;

use crate::batch::FileBatchStream;
use crate::batch::StorageBytesIterator;
use crate::batch_header::FileEmptyRecords;
use crate::file::FileBytesIterator;
use crate::index::Index;
use crate::util::log_path_get_offset;
use crate::util::OffsetError;

#[derive(Debug, thiserror::Error)]
pub enum LogValidationError {
    #[error("Invalid extension")]
    InvalidExtension,
    #[error("Invalid log name")]
    LogName(#[from] OffsetError),
    #[error("batch offset is less than base offset: {invalid_batch_offset}")]
    InvalidBaseOffsetMinimum { invalid_batch_offset: Offset },
    #[error("Offset not ordered")]
    OffsetNotOrdered,
    #[error("No batches")]
    NoBatches,
    #[error("Batch already exists")]
    ExistingBatch,
    #[error("Invalid Index: {offset} pos: {batch_file_pos} offset: {index_position}")]
    InvalidIndex {
        offset: Offset,
        batch_file_pos: u32,
        index_position: u32,
        diff_position: u32,
    },
}

/// Validation Log file is consistent with index file
#[derive(Debug)]
pub struct LogValidator {
    pub base_offset: Offset,
    file_path: PathBuf,
    pub batches: u32,
    pub last_valid_offset: Offset,
    pub duration: Duration,
    pub error: Option<LogValidationError>,
}

impl LogValidator {
    async fn validate_core<I, S, R>(
        path: impl AsRef<Path>,
        index: Option<&I>,
        skip_errors: bool,
        verbose: bool,
    ) -> Result<Self>
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
            batches: 0,
            duration: Duration::ZERO,
            error: None,
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

        val.validate_with_stream(batch_stream, index, skip_errors, verbose)
            .await?;

        val.duration = start_time.elapsed();
        Ok(val)
    }

    /// validate log file
    #[instrument(skip(self, index, skip_errors, verbose, batch_stream))]
    async fn validate_with_stream<I, R, S>(
        &mut self,
        mut batch_stream: FileBatchStream<R, S>,
        index: Option<&I>,
        skip_errors: bool,
        verbose: bool,
    ) -> Result<()>
    where
        I: Index,
        S: StorageBytesIterator,
        R: BatchRecords + Default + std::fmt::Debug,
    {
        let mut last_index_pos = 0;
        let mut last_batch_pos = 0;

        if batch_stream.is_invalid() {
            return Err(anyhow!("invalid batch stream"));
        }

        while let Some(batch_pos) = batch_stream.try_next().await? {
            let pos = batch_pos.get_pos();
            let batch = batch_pos.inner();

            let batch_offset = batch.get_base_offset();

            let header = batch.get_header();
            let offset_delta = header.last_offset_delta;

            if verbose {
                let diff_pos = pos - last_batch_pos;
                println!("found batch offset = {batch_offset}, pos = {pos}, diff_pos = {diff_pos}");
            }

            // offset relative to segment
            let delta_offset = batch_offset - self.base_offset;

            if let Some(index) = index {
                if let Some((offset, index_pos)) = index.find_offset(delta_offset as u32) {
                    if offset == delta_offset as u32 {
                        if verbose {
                            let diff_pos = index_pos - last_index_pos;
                            println!(
                                        "index offset = {offset}, idx_pos = {index_pos}, diff_pos={diff_pos}"
                                    );
                        }

                        if index_pos != pos {
                            if verbose {
                                let diff_index_batch_pos = index_pos - pos;
                                let diff_index_pos = index_pos - last_index_pos;
                                println!(
                                            "-- index mismatch: diff pos = {diff_index_batch_pos}, diff from prev index pos={diff_index_pos}"
                                        );
                            }

                            if !skip_errors {
                                self.error = Some(LogValidationError::InvalidIndex {
                                    offset: batch_offset,
                                    batch_file_pos: pos,
                                    index_position: index_pos,
                                    diff_position: index_pos - pos,
                                });
                                return Ok(());
                            }
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

            if batch_offset < self.base_offset {
                self.error = Some(LogValidationError::InvalidBaseOffsetMinimum {
                    invalid_batch_offset: batch_offset,
                });
                return Ok(());
            }

            last_batch_pos = pos;

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

            // last successfull offset
            self.last_valid_offset = batch_offset + offset_delta as Offset;

            // perform a simple json decoding

            self.batches += 1;
        }

        debug!(self.last_valid_offset, "found last offset");

        Ok(())
    }

    /// used by test
    #[instrument(skip(index, path))]
    async fn validate_segment<I, S>(
        path: impl AsRef<Path>,
        index: Option<&I>,
        skip_errors: bool,
        verbose: bool,
    ) -> Result<Offset>
    where
        I: Index,
        S: StorageBytesIterator,
    {
        match Self::validate_core::<I, S, FileEmptyRecords>(path, index, skip_errors, verbose).await
        {
            Ok(val) => {
                if val.last_valid_offset == -1 {
                    Ok(val.base_offset)
                } else {
                    Ok(val.last_valid_offset + 1)
                }
            }
            Err(err) => Err(err),
        }
    }

    /// Default validation using FileBytesIterator
    #[instrument(skip(index, path))]
    pub async fn validate<I>(
        path: impl AsRef<Path>,
        index: Option<&I>,
        skip_errors: bool,
        verbose: bool,
    ) -> Result<LogValidator>
    where
        I: Index,
    {
        LogValidator::validate_core::<I, FileBytesIterator, FileEmptyRecords>(
            path,
            index,
            skip_errors,
            verbose,
        )
        .await
    }
}

/// validate the file and find last offset
/// if file is not valid then return error

#[cfg(test)]
mod tests {

    use std::env::temp_dir;

    use flv_util::fixture::ensure_new_dir;
    use futures_lite::io::AsyncWriteExt;

    use fluvio_future::fs::BoundedFileSink;
    use fluvio_future::fs::BoundedFileOption;
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

        let validator = LogValidator::validate::<LogIndex>(&log_path, None, false, false)
            .await
            .expect("validate");
        assert_eq!(validator.last_valid_offset, BASE_OFFSET);
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

        let validator = LogValidator::validate::<LogIndex>(&log_path, None, false, true)
            .await
            .expect("validate");
        assert_eq!(validator.last_valid_offset, BASE_OFFSET + 5);
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
        assert!(
            LogValidator::validate::<LogIndex>(&test_file, None, false, false)
                .await
                .is_err()
        );
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
        let msm_result = LogValidator::validate_segment::<LogIndex, FileBytesIterator>(
            TEST_PATH, None, false, false,
        )
        .await
        .expect("validate");
        println!("header only took: {:#?}", header_time.elapsed());
        println!("validator: {:#?}", msm_result);

        /*
        let record_time = Instant::now();
        let _ = LogValidator::<_, MemoryRecords, LogIndex>::validate_segment(TEST_PATH, None, false, false)
            .await
            .expect("validate");
        println!("full scan took: {:#?}", record_time.elapsed());
        */
    }
}
