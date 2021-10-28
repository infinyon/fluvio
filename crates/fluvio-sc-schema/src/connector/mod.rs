pub use fluvio_controlplane_metadata::connector::*;

mod convert {

    use crate::{
        AdminSpec, NameFilter,
        objects::{Metadata, WatchResponse, ObjectApiWatchResponse},
    };
    use super::ManagedConnectorSpec;

    impl AdminSpec for ManagedConnectorSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = Self;
        type DeleteKey = String;

        fn create_decoder() -> crate::CreateDecoder {
            crate::CreateDecoder::MANAGED_CONNECTOR
        }
    }

    impl From<WatchResponse<ManagedConnectorSpec>> for ObjectApiWatchResponse {
        fn from(response: WatchResponse<ManagedConnectorSpec>) -> Self {
            ObjectApiWatchResponse::ManagedConnector(response)
        }
    }
}
