pub use fluvio_controlplane_metadata::spg::*;

mod convert {

    use crate::{
        AdminSpec, NameFilter,
        objects::{Metadata, WatchResponse, ObjectApiWatchResponse},
    };
    use super::SpuGroupSpec;

    impl AdminSpec for SpuGroupSpec {
        type ListFilter = NameFilter;

        type DeleteKey = String;

        type ListType = Metadata<Self>;

        type WatchResponseType = Self;
    }

    impl From<WatchResponse<SpuGroupSpec>> for ObjectApiWatchResponse {
        fn from(response: WatchResponse<SpuGroupSpec>) -> Self {
            ObjectApiWatchResponse::SpuGroup(response)
        }
    }
}
