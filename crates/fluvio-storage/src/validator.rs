use std::io::Error as IoError;
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

    let mut batch_stream = BatchHeaderStream::open(path).await?;
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

        if batch_base_offset <= last_offset {
            warn!(
                "batch offset is  {} is less than prev offset  {}",
                batch_base_offset, last_offset
            );
            return Err(LogValidationError::OffsetNotOrdered);
        }

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

    use futures_lite::io::AsyncWriteExt;

    use fluvio_future::fs::BoundedFileSink;
    use fluvio_future::fs::BoundedFileOption;
    use flv_util::fixture::ensure_clean_file;
    use dataplane::record::Record;
    use dataplane::batch::Batch;
    use dataplane::Offset;

    use crate::mut_records::MutFileRecords;
    use crate::config::ConfigOption;

    use super::validate;

    const PRODUCER: i64 = 33;

    pub fn create_batch(base_offset: Offset, records: u16) -> Batch {
        let mut batch = Batch::default();
        batch.set_base_offset(base_offset);
        let header = batch.get_mut_header();
        header.magic = 2;
        header.producer_id = PRODUCER;
        header.producer_epoch = -1;
        for _ in 0..records {
            let record = Record::new(vec![10, 20]);
            batch.add_record(record);
        }
        batch
    }

    const BASE_OFFSET: Offset = 301;

    #[fluvio_future::test]
    async fn test_validate_empty() {
        let test_file = temp_dir().join("00000000000000000301.log");
        ensure_clean_file(&test_file);

        let options = ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            ..Default::default()
        };

        let _ = MutFileRecords::open(BASE_OFFSET, &options)
            .await
            .expect("open");
        let next_offset = validate(&test_file).await.expect("validate");
        assert_eq!(next_offset, BASE_OFFSET);
    }

    const SUCCESS_BASE_OFFSET: Offset = 601;

    #[fluvio_future::test]
    async fn test_validate_success() {
        let test_file = temp_dir().join("00000000000000000601.log");
        ensure_clean_file(&test_file);

        let options = ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            ..Default::default()
        };

        let mut msg_sink = MutFileRecords::create(SUCCESS_BASE_OFFSET, &options)
            .await
            .expect("create");

        msg_sink
            .write_batch(&mut create_batch(SUCCESS_BASE_OFFSET, 2))
            .await
            .expect("write");
        msg_sink
            .write_batch(&mut create_batch(SUCCESS_BASE_OFFSET + 2, 3))
            .await
            .expect("write");

        let next_offset = validate(&test_file).await.expect("validate");
        assert_eq!(next_offset, SUCCESS_BASE_OFFSET + 5);
    }

    #[fluvio_future::test]
    async fn test_validate_offset() {
        let test_file = temp_dir().join("00000000000000000401.log");
        ensure_clean_file(&test_file);

        let options = ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            ..Default::default()
        };

        let mut msg_sink = MutFileRecords::create(401, &options).await.expect("create");

        msg_sink
            .write_batch(&mut create_batch(401, 0))
            .await
            .expect("create");
        msg_sink
            .write_batch(&mut create_batch(111, 1))
            .await
            .expect("create");

        assert!(validate(&test_file).await.is_err());
    }

    #[fluvio_future::test]
    async fn test_validate_invalid_contents() {
        let test_file = temp_dir().join("00000000000000000501.log");
        ensure_clean_file(&test_file);

        let options = ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            ..Default::default()
        };

        let mut msg_sink = MutFileRecords::create(501, &options)
            .await
            .expect("record created");
        msg_sink
            .write_batch(&mut create_batch(501, 2))
            .await
            .expect("create batch");

        // add some junk
        let mut f_sink = BoundedFileSink::create(&test_file, BoundedFileOption::default())
            .await
            .expect("open batch file");
        let bytes = vec![0x01, 0x02, 0x03];
        f_sink.write_all(&bytes).await.expect("write some junk");
        f_sink.flush().await.expect("flush");
        assert!(validate(&test_file).await.is_err());
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
