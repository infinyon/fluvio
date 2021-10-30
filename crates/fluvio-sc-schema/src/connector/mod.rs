pub use fluvio_controlplane_metadata::connector::*;

mod convert {

    use crate::{
        AdminSpec, NameFilter,
        objects::{
            CreateRequest, DeleteRequest, ListRequest, ListResponse, Metadata, ObjectFrom,
            ObjectTryFrom, WatchRequest, WatchResponse,
        },
    };
    use super::ManagedConnectorSpec;

    impl AdminSpec for ManagedConnectorSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = Self;
        type DeleteKey = String;
        const CREATE_TYPE: u8 =3;

        fn create_decoder() -> crate::CreateDecoder {
            crate::CreateDecoder::ManagedConnector
        }
    }

    ObjectFrom!(CreateRequest, ManagedConnector, Create);
    ObjectFrom!(WatchRequest, ManagedConnector);
    ObjectFrom!(WatchResponse, ManagedConnector);
    ObjectFrom!(ListRequest, ManagedConnector);
    ObjectFrom!(ListResponse, ManagedConnector);
    ObjectFrom!(DeleteRequest, ManagedConnector);

    ObjectTryFrom!(WatchResponse, ManagedConnector);
    ObjectTryFrom!(ListResponse, ManagedConnector);
}
