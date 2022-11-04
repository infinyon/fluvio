pub use fluvio_controlplane_metadata::connector::*;

mod convert {

    use crate::{
        AdminSpec, CreatableAdminSpec, DeletableAdminSpec,
        objects::{
            CreateFrom, DeleteRequest, ListRequest, ListResponse, ObjectFrom, ObjectTryFrom,
            WatchRequest, WatchResponse,
        },
    };
    use super::ManagedConnectorSpec;

    impl AdminSpec for ManagedConnectorSpec {}

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
