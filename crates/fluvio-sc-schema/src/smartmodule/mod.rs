pub use fluvio_controlplane_metadata::smartmodule::*;

mod convert {

    use crate::{
        AdminSpec, CreatableAdminSpec, DeletableAdminSpec, NameFilter,
        objects::{
            CreateFrom, DeleteRequest, ListRequest, ListResponse, Metadata, ObjectFrom,
            ObjectTryFrom, WatchRequest, WatchResponse,
        },
    };
    use super::SmartModuleSpec;

    impl AdminSpec for SmartModuleSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = Self;
        type ListType = Metadata<Self>;
    }

    impl CreatableAdminSpec for SmartModuleSpec {
        const CREATE_TYPE: u8 = 4;
    }

    impl DeletableAdminSpec for SmartModuleSpec {
        type DeleteKey = String;
    }

    CreateFrom!(SmartModuleSpec, SmartModule);
    ObjectFrom!(WatchRequest, SmartModule);
    ObjectFrom!(WatchResponse, SmartModule);
    ObjectFrom!(ListRequest, SmartModule);
    ObjectFrom!(ListResponse, SmartModule);
    ObjectFrom!(DeleteRequest, SmartModule);

    ObjectTryFrom!(WatchResponse, SmartModule);
    ObjectTryFrom!(ListResponse, SmartModule);

    use super::SmartModuleMetadataSpec;

    /*
    impl AdminSpec for SmartModuleMetadataSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = Self;
        type ListType = Metadata<Self>;
    }
    */
    ObjectFrom!(ListRequest, SmartModuleMetadata);
    ObjectFrom!(ListResponse, SmartModuleMetadata);
    ObjectTryFrom!(ListResponse, SmartModuleMetadata);
}
