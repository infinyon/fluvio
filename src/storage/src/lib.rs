#[cfg(test)]
mod fixture;

pub mod batch;
pub mod batch_header;
mod checkpoint;
mod error;
mod records;
mod index;
mod mut_records;
mod mut_index;
mod range_map;
mod replica;
pub mod segment;
mod util;
mod validator;
pub mod config;

pub use crate::error::StorageError;
pub use crate::records::FileRecordsSlice;
pub use crate::index::LogIndex;
pub use crate::index::OffsetPosition;
pub use crate::replica::FileReplica;
pub use crate::segment::SegmentSlice;

use dataplane::{ErrorCode, Offset};
use dataplane::fetch::FilePartitionResponse;
use fluvio_future::file_slice::AsyncFileSlice;

pub trait Captures<'a> {}
impl<'a, T: ?Sized> Captures<'a> for T {}

/// output from storage is represented as slice
pub trait SlicePartitionResponse {
    fn set_hw(&mut self, offset: i64);

    fn set_last_stable_offset(&mut self, offset: i64);

    fn set_log_start_offset(&mut self, offset: i64);

    fn set_slice(&mut self, slice: AsyncFileSlice);

    fn set_error_code(&mut self, error: ErrorCode);
}

impl SlicePartitionResponse for FilePartitionResponse {
    fn set_hw(&mut self, offset: i64) {
        self.high_watermark = offset;
    }

    fn set_last_stable_offset(&mut self, offset: i64) {
        self.last_stable_offset = offset;
    }

    fn set_log_start_offset(&mut self, offset: i64) {
        self.log_start_offset = offset;
    }

    fn set_slice(&mut self, slice: AsyncFileSlice) {
        self.records = slice.into();
    }

    fn set_error_code(&mut self, error: ErrorCode) {
        self.error_code = error;
    }
}

pub trait ReplicaStorage {
    /// high water mark offset (records that has been replicated)
    fn get_hw(&self) -> Offset;

    /// log end offset ( records that has been stored)
    fn get_leo(&self) -> Offset;
}
