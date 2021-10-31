pub use fluvio_controlplane_metadata::connector::*;

mod convert {

    use crate::{
        AdminSpec, CreatableAdminSpec, DeletableAdminSpec, NameFilter,
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
    }

    impl CreatableAdminSpec for ManagedConnectorSpec {
        const CREATE_TYPE: u8 = 3;
    }

    impl DeletableAdminSpec for ManagedConnectorSpec {
        type DeleteKey = String;
    }

    ObjectFrom!(CreateRequest, ManagedConnector);
    ObjectFrom!(WatchRequest, ManagedConnector);
    ObjectFrom!(WatchResponse, ManagedConnector);
    ObjectFrom!(ListRequest, ManagedConnector);
    ObjectFrom!(ListResponse, ManagedConnector);
    ObjectFrom!(DeleteRequest, ManagedConnector);

    ObjectTryFrom!(WatchResponse, ManagedConnector);
    ObjectTryFrom!(ListResponse, ManagedConnector);
}
