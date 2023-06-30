use std::collections::VecDeque;
use std::os::unix::io::RawFd;
use std::io::{Error as IoError, ErrorKind, Cursor};

use fluvio_protocol::{Decoder, Version};
use fluvio_smartmodule::FluvioRecord as Record;
use fluvio_types::Timestamp;
use tracing::{warn, debug};
use nix::sys::uio::pread;

use fluvio_protocol::record::{Batch, Offset, BATCH_FILE_HEADER_SIZE, BATCH_HEADER_SIZE};
use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_compression::{Compression, CompressionError};

use super::batch::SmartModuleInputBatch;

// only encode information necessary to decode batches efficiently
pub struct FileBatch {
    pub(crate) batch: Batch,
    pub(crate) records: Vec<u8>,
}

impl SmartModuleInputBatch for FileBatch {
    fn records(&self) -> &Vec<u8> {
        &self.records
    }

    fn base_offset(&self) -> Offset {
        self.batch.base_offset
    }

    fn offset_delta(&self) -> i32 {
        self.batch.header.last_offset_delta
    }

    fn get_compression(&self) -> Result<Compression, CompressionError> {
        self.batch.get_compression()
    }

    fn base_timestamp(&self) -> i64 {
        self.batch.get_base_timestamp()
    }
}

/// Iterator that returns batch from file
pub struct FileBatchIterator {
    fd: RawFd,
    offset: i64,
    end: i64,
}

impl FileBatchIterator {
    #[allow(unused)]
    pub fn new(fd: RawFd, offset: i64, len: i64) -> Self {
        Self {
            fd,
            offset,
            end: offset + len,
        }
    }

    pub fn from_raw_slice(slice: AsyncFileSlice) -> Self {
        use std::os::unix::io::AsRawFd;
        let offset = slice.position() as i64;
        Self {
            fd: slice.as_raw_fd(),
            offset,
            end: offset + slice.len() as i64,
        }
    }
}

impl Iterator for FileBatchIterator {
    type Item = Result<FileBatch, IoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.end {
            return None;
        }

        let mut header = vec![0u8; BATCH_FILE_HEADER_SIZE];
        let bytes_read = match pread(self.fd, &mut header, self.offset)
            .map_err(|err| IoError::new(ErrorKind::Other, format!("pread error {err}")))
        {
            Ok(bytes) => bytes,
            Err(err) => return Some(Err(err)),
        };

        if bytes_read < header.len() {
            warn!(bytes_read, header_len = header.len());
            return Some(Err(IoError::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not eough for batch header {} out of {}",
                    bytes_read,
                    header.len()
                ),
            )));
        }

        let mut batch: Batch = Batch::default();
        if let Err(err) = batch.decode_from_file_buf(&mut Cursor::new(header), 0) {
            return Some(Err(IoError::new(
                ErrorKind::Other,
                format!("decodinge batch header error {err}"),
            )));
        }

        let remainder = batch.batch_len as usize - BATCH_HEADER_SIZE;

        debug!(
            file_offset = self.offset,
            base_offset = batch.base_offset,
            "fbatch header"
        );

        let mut raw_records = vec![0u8; remainder];

        self.offset += BATCH_FILE_HEADER_SIZE as i64;

        let bytes_read = match pread(self.fd, &mut raw_records, self.offset)
            .map_err(|err| IoError::new(ErrorKind::Other, format!("pread error {err}")))
        {
            Ok(bytes) => bytes,
            Err(err) => return Some(Err(err)),
        };

        if bytes_read < raw_records.len() {
            warn!(bytes_read, record_len = raw_records.len());
            return Some(Err(IoError::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not enough for batch records {} out of {}",
                    bytes_read,
                    raw_records.len()
                ),
            )));
        }

        let compression = match batch.get_compression() {
            Ok(compression) => compression,
            Err(err) => {
                return Some(Err(IoError::new(
                    ErrorKind::Other,
                    format!("unknown compression value for batch {err}"),
                )))
            }
        };

        let records = match compression.uncompress(&raw_records) {
            Ok(Some(records)) => records,
            Ok(None) => raw_records,
            Err(err) => {
                return Some(Err(IoError::new(
                    ErrorKind::Other,
                    format!("uncompress error {err}"),
                )))
            }
        };

        self.offset += bytes_read as i64;

        debug!(file_offset = self.offset, "fbatch end");

        Some(Ok(FileBatch { batch, records }))
    }
}

/// Iterator that converts an iterator over file batches to an iterator over record items.
/// RecordItem is a record with resolved offset and timestamp.
pub struct FileRecordIterator<T: Iterator<Item = Result<FileBatch, IoError>>> {
    batch_iterator: T,
    batch: VecDeque<RecordItem>,
    version: Version,
}

impl<T: Iterator<Item = Result<FileBatch, IoError>>> FileRecordIterator<T> {
    pub fn new(batch_iterator: T, version: Version) -> Self {
        Self {
            batch_iterator,
            batch: VecDeque::new(),
            version,
        }
    }
}

impl<T: Iterator<Item = Result<FileBatch, IoError>>> Iterator for FileRecordIterator<T> {
    type Item = Result<RecordItem, IoError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.batch.pop_front() {
            Some(r) => Some(Ok(r)),
            None => {
                let next_batch = match self.batch_iterator.next()? {
                    Ok(b) => b,
                    Err(err) => return Some(Err(err)),
                };

                let base_offset = next_batch.batch.base_offset;
                let base_timestamp = next_batch.batch.header.first_timestamp;

                let mut records: Vec<Record> = vec![];
                if let Err(err) = Decoder::decode(
                    &mut records,
                    &mut std::io::Cursor::new(next_batch.records),
                    self.version,
                ) {
                    return Some(Err(err));
                }
                self.batch.append(
                    &mut records
                        .into_iter()
                        .map(|record| {
                            let offset = base_offset + record.get_header().offset_delta();
                            let timestamp =
                                base_timestamp + record.get_header().get_timestamp_delta();
                            RecordItem {
                                record,
                                offset,
                                timestamp,
                            }
                        })
                        .collect(),
                );
                self.batch.pop_front().map(Ok)
            }
        }
    }
}

#[derive(Debug)]
pub struct RecordItem {
    pub record: Record,
    pub offset: Offset,
    pub timestamp: Timestamp,
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::time::SystemTime;
    use std::fs::File;
    use std::env::temp_dir;
    use std::os::unix::io::AsRawFd;

    use fluvio_future::task::run_block_on;
    use fluvio_protocol::record::RecordSet;
    use fluvio_storage::{FileReplica, ReplicaStorage};
    use fluvio_storage::config::{StorageConfigBuilder, ReplicaConfigBuilder};

    use super::*;

    #[test]
    fn test_file_record_iterator() -> anyhow::Result<()> {
        //given
        let base_dir = temp_dir().join("test_file_record_iterator");
        let mut replica = run_block_on(FileReplica::create_or_load_with_storage(
            format!(
                "test_file_record_iterator_{}",
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_millis()
            ),
            Default::default(),
            Default::default(),
            ReplicaConfigBuilder::default().base_dir(base_dir).build(),
            Arc::new(StorageConfigBuilder::default().build()?),
        ))?;

        let mut batch1 = Batch::default();
        batch1.header.first_timestamp = 100;

        batch1.add_record(Record::new("1"));
        batch1.add_record(Record::new("2"));
        batch1.mut_records()[0].preamble.set_timestamp_delta(1);
        batch1.mut_records()[1].preamble.set_timestamp_delta(2);

        let mut batch2 = Batch::default();
        batch2.header.first_timestamp = 200;
        batch2.base_offset = 2;

        batch2.add_record(Record::new("3"));
        batch2.add_record(Record::new("4"));
        batch2.mut_records()[0].preamble.set_timestamp_delta(1);
        batch2.mut_records()[1].preamble.set_timestamp_delta(2);

        let mut records = RecordSet {
            batches: vec![batch1, batch2],
        };
        run_block_on(replica.write_recordset(&mut records, false))?;

        //when
        let slice = run_block_on(replica.read_partition_slice(
            0,
            u32::MAX,
            fluvio::Isolation::ReadUncommitted,
        ))?;
        let file_slice = slice
            .file_slice
            .ok_or_else(|| anyhow::anyhow!("expected file slice"))?;

        let record_iter = FileRecordIterator::new(FileBatchIterator::from_raw_slice(file_slice), 0);
        let records: Vec<RecordItem> =
            record_iter.collect::<Result<Vec<RecordItem>, std::io::Error>>()?;

        //then
        assert_eq!(records.len(), 4);
        assert_eq!(std::str::from_utf8(records[0].record.value())?, "1");
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].timestamp, 101);

        assert_eq!(std::str::from_utf8(records[1].record.value())?, "2");
        assert_eq!(records[1].offset, 1);
        assert_eq!(records[1].timestamp, 102);

        assert_eq!(std::str::from_utf8(records[2].record.value())?, "3");
        assert_eq!(records[2].offset, 2);
        assert_eq!(records[2].timestamp, 201);

        assert_eq!(std::str::from_utf8(records[3].record.value())?, "4");
        assert_eq!(records[3].offset, 3);
        assert_eq!(records[3].timestamp, 202);

        Ok(())
    }

    #[test]
    fn test_file_record_iterator_error_propagated() -> anyhow::Result<()> {
        //given
        let path = temp_dir().join("test_file_record_iterator_error_propagated");
        let bytes = b"some data";
        std::fs::write(&path, bytes)?;

        let read_only = File::open(path)?;
        let fd = read_only.as_raw_fd();

        let file_slice = AsyncFileSlice::new(fd, 0, bytes.len() as u64);

        //when
        let record_iter = FileRecordIterator::new(FileBatchIterator::from_raw_slice(file_slice), 0);
        let res = record_iter.collect::<Result<Vec<RecordItem>, std::io::Error>>();

        //then
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::UnexpectedEof);

        Ok(())
    }
}
