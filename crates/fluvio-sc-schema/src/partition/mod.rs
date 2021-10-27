pub use fluvio_controlplane_metadata::partition::*;

mod convert {

    use crate::{
        AdminSpec, NameFilter,
        objects::{Metadata, WatchResponse, ObjectApiWatchResponse},
    };
    use super::*;

    impl AdminSpec for PartitionSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = Self;

        type DeleteKey = String;

        type ListType = Metadata<Self>;
    }

    impl From<WatchResponse<PartitionSpec>> for ObjectApiWatchResponse {
        fn from(response: WatchResponse<PartitionSpec>) -> Self {
            ObjectApiWatchResponse::Partition(response)
        }
    }
}
