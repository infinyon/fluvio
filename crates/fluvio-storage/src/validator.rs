use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;

use tracing::{debug, warn, trace};

use dataplane::Offset;

use crate::batch_header::BatchHeaderStream;
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
}

/// validate the file and find last offset
/// if file is not valid then return error
#[allow(dead_code)]
pub async fn validate<P>(path: P) -> Result<Offset, LogValidationError>
where
    P: AsRef<Path>,
{
    let file_path = path.as_ref();
    let base_offset = log_path_get_offset(file_path)?;
    let file_name = file_path.display().to_string();

    debug!(
        %file_name,
        base_offset,
        "validating",
    );

    let mut batch_stream = match BatchHeaderStream::open(path).await {
        Ok(batch_stream) => batch_stream,
        Err(err) => match err.kind() {
            ErrorKind::UnexpectedEof => {
                debug!(%file_name, "empty");
                return Ok(base_offset);
            }
            _ => return Err(err.into()),
        },
    };

    let mut last_offset: Offset = -1;

    while let Some(batch_pos) = batch_stream.next().await {
        let batch_base_offset = batch_pos.get_batch().get_base_offset();
        let header = batch_pos.get_batch().get_header();
        let offset_delta = header.last_offset_delta;

        trace!(batch_base_offset, offset_delta, "found batch");

        if batch_base_offset < base_offset {
            warn!(
                "batch base offset: {} is less than base offset: {} path: {:#?}",
                batch_base_offset, base_offset, file_name
            );
            return Err(LogValidationError::BaseOff);
        }

        /*
        if batch_base_offset <= last_offset {
            warn!(
                "batch offset is  {} is less than prev offset  {}",
                batch_base_offset, last_offset
            );
            return Err(LogValidationError::OffsetNotOrdered);
        }
        */

        last_offset = batch_base_offset + offset_delta as Offset;
    }

    if let Some(err) = batch_stream.invalid() {
        return Err(err.into());
    }

    if last_offset == -1 {
        trace!("no batch found, returning last offset delta 0");
        return Ok(base_offset);
    }

    debug!(last_offset, "found last offset");
    Ok(last_offset + 1)
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
    use crate::config::ConfigOption;
    use crate::records::FileRecords;
    use crate::validator::LogValidationError;

    use super::validate;

    #[fluvio_future::test]
    async fn test_validate_empty() {
        const BASE_OFFSET: Offset = 301;

        let test_dir = temp_dir().join("validation_empty");
        ensure_new_dir(&test_dir).expect("new");

        let options = ConfigOption {
            base_dir: test_dir,
            segment_max_bytes: 1000,
            ..Default::default()
        };

        let log_records = MutFileRecords::create(BASE_OFFSET, &options)
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

        let options = ConfigOption {
            base_dir: test_dir,
            segment_max_bytes: 1000,
            ..Default::default()
        };

        let mut msg_sink = MutFileRecords::create(BASE_OFFSET, &options)
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

        let options = ConfigOption {
            base_dir: test_dir,
            segment_max_bytes: 1000,
            ..Default::default()
        };

        let mut msg_sink = MutFileRecords::create(OFFSET, &options)
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

    use super::validate;

    const TEST_PATH: &str =
        "/tmp/fluvio-large-data/spu-logs-5002/longevity-0/00000000000000000000.log";
    #[fluvio_future::test]
    async fn perf_test() {
        println!("starting test");
        let write_time = Instant::now();
        let last_offset = validate(&TEST_PATH).await.expect("validate");
        let time = write_time.elapsed();
        println!("took: {:#?}", time);
        println!("next offset: {}", last_offset);
    }
}
