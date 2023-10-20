use fluvio_types::Timestamp;

use fluvio_protocol::record::Offset;
use fluvio_compression::{Compression, CompressionError};

use super::batch::SmartModuleInputBatch;

use fluvio_storage::iterators::FileBatch;

impl SmartModuleInputBatch for FileBatch {
    fn records(&self) -> &Vec<u8> {
        &self.records
    }

    fn base_offset(&self) -> Offset {
        self.batch.base_offset
    }

    fn base_timestamp(&self) -> Timestamp {
        self.batch.get_base_timestamp()
    }

    fn offset_delta(&self) -> i32 {
        self.batch.header.last_offset_delta
    }

    fn get_compression(&self) -> Result<Compression, CompressionError> {
        self.batch.get_compression()
    }
}
