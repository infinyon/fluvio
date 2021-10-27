pub use fluvio_controlplane_metadata::smartmodule::*;

mod convert {

    use crate::{
        AdminSpec, NameFilter,
        objects::{Metadata, WatchResponse, ObjectApiWatchResponse},
    };
    use super::SmartModuleSpec;

    impl AdminSpec for SmartModuleSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = Self;

        type DeleteKey = String;

        type ListType = Metadata<Self>;
    }

    impl From<WatchResponse<SmartModuleSpec>> for ObjectApiWatchResponse {
        fn from(response: WatchResponse<SmartModuleSpec>) -> Self {
            ObjectApiWatchResponse::SmartModule(response)
        }
    }
}
