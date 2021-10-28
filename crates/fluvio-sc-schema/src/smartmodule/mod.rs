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

        fn create_decoder() -> crate::CreateDecoder {
            crate::CreateDecoder::SMART_MODULE
        }
    }

    impl From<WatchResponse<SmartModuleSpec>> for ObjectApiWatchResponse {
        fn from(response: WatchResponse<SmartModuleSpec>) -> Self {
            ObjectApiWatchResponse::SmartModule(response)
        }
    }
}
