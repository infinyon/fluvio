use std::io::Error as IoError;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;

use fluvio_protocol::record::BatchRecords;
use tracing::error;
use tracing::instrument;
use tracing::{debug, warn};

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

    #[error("Invalid Index: {offset} pos: {batch_file_pos} offset: {index_position}")]
    InvalidIndex {
        offset: Offset,
        batch_file_pos: u32,
        index_position: u32,
        diff_position: u32,
    },
}

/// Validation Log file
#[derive(Debug)]
pub struct LogValidator<P, R, I, S = FileBytesIterator> {
    pub base_offset: Offset,
    file_path: PathBuf,
    pub batches: u32,
    pub last_offset: Offset,
    pub success: u32,
    pub failed: u32,
    data1: PhantomData<P>,
    data2: PhantomData<R>,
    data3: PhantomData<I>,
    data4: PhantomData<S>,
}

impl<P, R, I, S> LogValidator<P, R, I, S>
where
    P: AsRef<Path>,
    I: Index,
    S: StorageBytesIterator,
    R: BatchRecords + Default + std::fmt::Debug,
{
    async fn validate_core(
        path: P,
        index: Option<&I>,
        skip_errors: bool,
        verbose: bool,
    ) -> Result<Self, LogValidationError> {
        let file_path = path.as_ref().to_path_buf();
        let mut val = Self {
            base_offset: log_path_get_offset(&file_path)?,
            last_offset: -1,
            file_path,
            batches: 0,
            success: 0,
            failed: 0,
            data1: PhantomData,
            data2: PhantomData,
            data3: PhantomData,
            data4: PhantomData,
        };

        debug!(
            file_name = %val.file_path.display(),
            val.base_offset,
            "validating",
        );

        let batch_stream: FileBatchStream<R, S> = match FileBatchStream::open(&val.file_path).await
        {
            Ok(batch_stream) => batch_stream,
            Err(err) => match err.kind() {
                ErrorKind::UnexpectedEof => return Err(LogValidationError::Empty(val.base_offset)),
                _ => return Err(err.into()),
            },
        };

        val.validate_with_stream(batch_stream, index, skip_errors, verbose)
            .await?;
        Ok(val)
    }

    /// open validator on the log file path
    #[instrument(skip(self, index, skip_errors, verbose, batch_stream))]
    pub async fn validate_with_stream(
        &mut self,
        mut batch_stream: FileBatchStream<R, S>,
        index: Option<&I>,
        skip_errors: bool,
        verbose: bool,
    ) -> Result<(), LogValidationError> {
        let mut last_index_pos = 0;
        let mut last_batch_pos = 0;

        while let Some(batch_pos) = batch_stream.next().await {
            let batch_offset = batch_pos.get_batch().get_base_offset();
            let pos = batch_pos.get_pos();

            let header = batch_pos.get_batch().get_header();
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
                                return Err(LogValidationError::InvalidIndex {
                                    offset: batch_offset,
                                    batch_file_pos: batch_pos.get_pos(),
                                    index_position: index_pos,
                                    diff_position: index_pos - batch_pos.get_pos(),
                                });
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
                warn!(
                    "batch base offset: {} is less than base offset: {} path: {:#?}",
                    batch_offset,
                    self.base_offset,
                    self.file_path.display()
                );
                return Err(LogValidationError::BaseOff);
            }

            last_batch_pos = batch_pos.get_pos();

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

            self.last_offset = batch_offset + offset_delta as Offset;

            // perform a simple json decoding

            self.batches += 1;
        }

        if let Some(err) = batch_stream.invalid() {
            return Err(err.into());
        }

        debug!(self.last_offset, "found last offset");

        Ok(())
    }

    fn next_offset(&self) -> Offset {
        if self.last_offset == -1 {
            return self.base_offset;
        }

        self.last_offset + 1
    }
}

impl<P, I, S> LogValidator<P, FileEmptyRecords, I, S>
where
    P: AsRef<Path>,
    I: Index,
    S: StorageBytesIterator,
{
    /// generalized validation using byte iterator
    #[instrument(skip(index, path))]
    async fn validate_segment(
        path: P,
        index: Option<&I>,
        skip_errors: bool,
        verbose: bool,
    ) -> Result<Offset, LogValidationError> {
        match Self::validate_core(path, index, skip_errors, verbose).await {
            Ok(val) => Ok(val.next_offset()),
            Err(LogValidationError::Empty(base_offset)) => Ok(base_offset),
            Err(err) => Err(err),
        }
    }
}

/// Default validation using FileBytesIterator
#[instrument(skip(index, path))]
pub async fn validate<P, I>(
    path: P,
    index: Option<&I>,
    skip_errors: bool,
    verbose: bool,
) -> Result<Offset, LogValidationError>
where
    P: AsRef<Path>,
    I: Index,
{
    LogValidator::<P, FileEmptyRecords, I, FileBytesIterator>::validate_segment(
        path,
        index,
        skip_errors,
        verbose,
    )
    .await
}

/// validate the file and find last offset
/// if file is not valid then return error

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::io::ErrorKind;

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
    use crate::validator::LogValidationError;

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

        let next_offset = validate::<_, LogIndex>(&log_path, None, false, false)
            .await
            .expect("validate");
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

        let next_offset = validate::<_, LogIndex>(&log_path, None, false, true)
            .await
            .expect("validate");
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
        match validate::<_, LogIndex>(&test_file, None, false, false).await {
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

    use crate::{batch_header::FileEmptyRecords, LogIndex};

    use super::*;

    //#[fluvio_future::test]
    #[allow(unused)]
    // execute this with: `cargo test perf_test -p fluvio-storage --release -- --nocapture`
    async fn perf_test() {
        const TEST_PATH: &str = "/tmp/perf/00000000000000000000.log";

        println!("starting test");
        let header_time = Instant::now();
        let msm_result = LogValidator::<_, FileEmptyRecords, LogIndex>::validate_segment(
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
