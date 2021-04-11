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

#[cfg(feature = "fixture")]
pub mod fixture;

pub use crate::error::StorageError;
pub use crate::records::FileRecordsSlice;
pub use crate::index::LogIndex;
pub use crate::index::OffsetPosition;
pub use crate::replica::FileReplica;
pub use crate::segment::SegmentSlice;
pub use inner::*;
mod inner {
    use async_trait::async_trait;

    use dataplane::{ErrorCode, Isolation, Offset, ReplicaKey};
    use dataplane::fetch::FilePartitionResponse;
    use dataplane::record::RecordSet;
    use fluvio_future::file_slice::AsyncFileSlice;
    use fluvio_types::SpuId;

    use crate::StorageError;

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

        #[allow(deprecated)]
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

    /// some storage configuration
    pub trait ReplicaStorageConfig {}

    #[async_trait]
    pub trait ReplicaStorage: Sized {
        type Config: ReplicaStorageConfig;

        /// create new storage area,
        /// if there exists replica state, this should restore state
        async fn create(
            replica: &ReplicaKey,
            spu: SpuId,
            config: Self::Config,
        ) -> Result<Self, StorageError>;

        /// high water mark offset (records that has been replicated)
        fn get_hw(&self) -> Offset;

        /// log end offset ( records that has been stored)
        fn get_leo(&self) -> Offset;

        fn get_log_start_offset(&self) -> Offset;

        /// read partition slice
        /// return hw and leo
        async fn read_partition_slice<P>(
            &self,
            offset: Offset,
            max_len: u32,
            isolation: Isolation,
            partition_response: &mut P,
        ) -> (Offset, Offset)
        where
            P: SlicePartitionResponse + Send;

        /// write record set
        async fn write_recordset(
            &mut self,
            records: &mut RecordSet,
            update_highwatermark: bool,
        ) -> Result<(), StorageError>;

        async fn update_high_watermark(&mut self, offset: Offset) -> Result<bool, StorageError>;

        /// permanently remove
        async fn remove(&self) -> Result<(), StorageError>;
    }
}
