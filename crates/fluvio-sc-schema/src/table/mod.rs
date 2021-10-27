pub use fluvio_controlplane_metadata::table::*;

mod convert {

    use crate::{
        AdminSpec, NameFilter,
        objects::{Metadata, WatchResponse, ObjectApiWatchResponse},
    };
    use super::TableSpec;

    impl AdminSpec for TableSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = Self;

        type DeleteKey = String;
    }

    impl From<WatchResponse<TableSpec>> for ObjectApiWatchResponse {
        fn from(response: WatchResponse<TableSpec>) -> Self {
            ObjectApiWatchResponse::Table(response)
        }
    }
}
