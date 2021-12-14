pub mod batch;
pub mod batch_header;
mod checkpoint;
mod error;
mod records;
mod index;
mod mut_records;
mod mut_index;
mod segments;
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

    #[derive(Debug, Clone, PartialEq)]
    pub struct OffsetInfo {
        pub hw: Offset,
        pub leo: Offset,
    }

    impl Default for OffsetInfo {
        fn default() -> Self {
            Self { hw: -1, leo: -1 }
        }
    }

    impl From<(Offset, Offset)> for OffsetInfo {
        fn from(value: (Offset, Offset)) -> Self {
            Self::new(value.0, value.1)
        }
    }

    impl OffsetInfo {
        pub fn new(leo: Offset, hw: Offset) -> Self {
            assert!(leo >= hw, "end offset >= high watermark");
            Self { hw, leo }
        }

        /// get isolation offset
        pub fn isolation(&self, isolation: &Isolation) -> Offset {
            match isolation {
                Isolation::ReadCommitted => self.hw,
                Isolation::ReadUncommitted => self.leo,
            }
        }

        /// check if offset contains valid value
        ///  invalid if either hw or leo is -1
        ///  or if hw > leo
        pub fn is_valid(&self) -> bool {
            !(self.hw == -1 || self.leo == -1) && self.leo >= self.hw
        }

        /// update hw, leo
        /// return true if there was change
        /// otherwise false
        pub fn update(&mut self, other: &Self) -> bool {
            let mut change = false;
            if other.hw > self.hw {
                self.hw = other.hw;
                change = true;
            }
            if other.leo > self.leo {
                self.leo = other.leo;
                change = true;
            }
            change
        }

        /// check if we are newer than other
        pub fn newer(&self, other: &Self) -> bool {
            self.leo > other.leo || self.hw > other.hw
        }

        pub fn is_same(&self, other: &Self) -> bool {
            self.hw == other.hw && self.leo == other.leo
        }

        /// is hw fully caught with leo
        pub fn is_committed(&self) -> bool {
            self.leo == self.hw
        }
    }

    use crate::StorageError;

    /// output from storage is represented as slice
    pub trait SlicePartitionResponse {
        fn set_hw(&mut self, offset: i64);

        fn set_log_start_offset(&mut self, offset: i64);

        fn set_slice(&mut self, slice: AsyncFileSlice);

        fn set_error_code(&mut self, error: ErrorCode);
    }

    impl SlicePartitionResponse for FilePartitionResponse {
        fn set_hw(&mut self, offset: i64) {
            self.high_watermark = offset;
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
        async fn create_or_load(
            replica: &ReplicaKey,
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
        ) -> OffsetInfo
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

    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn test_offset_validation() {
            assert!(!OffsetInfo::default().is_valid());

            assert!(!OffsetInfo { hw: 2, leo: 1 }.is_valid());

            assert!(OffsetInfo { hw: 2, leo: 3 }.is_valid());

            assert!(OffsetInfo { hw: 0, leo: 0 }.is_valid());

            assert!(OffsetInfo { hw: 4, leo: 4 }.is_valid());

            assert!(!OffsetInfo { hw: -1, leo: 3 }.is_valid());
        }

        #[test]
        fn test_offset_newer() {
            assert!(!OffsetInfo { hw: 1, leo: 2 }.newer(&OffsetInfo { hw: 2, leo: 2 }));

            assert!(OffsetInfo { hw: 2, leo: 10 }.newer(&OffsetInfo { hw: 0, leo: 0 }));
        }

        #[test]
        fn test_offset_update() {
            assert!(!OffsetInfo { hw: 1, leo: 2 }.update(&OffsetInfo { hw: 0, leo: 0 }));

            assert!(OffsetInfo { hw: 1, leo: 2 }.update(&OffsetInfo { hw: 1, leo: 3 }));
        }
    }
}
