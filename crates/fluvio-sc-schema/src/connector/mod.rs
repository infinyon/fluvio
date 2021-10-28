pub use fluvio_controlplane_metadata::connector::*;

mod convert {

    use crate::{
        AdminSpec, NameFilter,
        objects::{
            CreateRequest, ListRequest, ListResponse, Metadata, ObjectFrom, ObjectTryFrom,
            WatchRequest, WatchResponse,
        },
    };
    use super::ManagedConnectorSpec;

    impl AdminSpec for ManagedConnectorSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = Self;
        type DeleteKey = String;

        fn create_decoder() -> crate::CreateDecoder {
            crate::CreateDecoder::ManagedConnector
        }
    }

    ObjectFrom!(CreateRequest, ManagedConnector, Create);
    ObjectFrom!(WatchRequest, ManagedConnector);
    ObjectFrom!(WatchResponse, ManagedConnector);

    ObjectFrom!(ListRequest, ManagedConnector);
    ObjectFrom!(ListResponse, ManagedConnector);

    ObjectTryFrom!(WatchResponse, ManagedConnector);
    ObjectTryFrom!(ListResponse, ManagedConnector);
}
