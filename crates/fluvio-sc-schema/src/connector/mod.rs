pub use fluvio_controlplane_metadata::connector::*;

mod convert {

    use crate::{
        AdminSpec, CreatableAdminSpec, DeletableAdminSpec, NameFilter,
        objects::{
            CreateFrom, DeleteRequest, ListRequest, ListResponse, Metadata, ObjectFrom,
            ObjectTryFrom, WatchRequest, WatchResponse,
        },
    };
    use super::ManagedConnectorSpec;

    impl AdminSpec for ManagedConnectorSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = Self;

        fn convert_from<C: fluvio_controlplane_metadata::core::MetadataItem>(
            obj: &fluvio_controlplane_metadata::store::MetadataStoreObject<Self, C>,
        ) -> Self::ListType {
            obj.clone().into()
        }
    }

    impl CreatableAdminSpec for ManagedConnectorSpec {
        const CREATE_TYPE: u8 = 3;
    }

    impl DeletableAdminSpec for ManagedConnectorSpec {
        type DeleteKey = String;
    }

    CreateFrom!(ManagedConnectorSpec, ManagedConnector);
    ObjectFrom!(WatchRequest, ManagedConnector);
    ObjectFrom!(WatchResponse, ManagedConnector);
    ObjectFrom!(ListRequest, ManagedConnector);
    ObjectFrom!(ListResponse, ManagedConnector);
    ObjectFrom!(DeleteRequest, ManagedConnector);

    ObjectTryFrom!(WatchResponse, ManagedConnector);
    ObjectTryFrom!(ListResponse, ManagedConnector);
}
