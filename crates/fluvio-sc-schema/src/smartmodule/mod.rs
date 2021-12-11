pub use fluvio_controlplane_metadata::smartmodule::*;

mod convert {

    use fluvio_controlplane_metadata::smartmodule::SmartModuleMetadataSpec;

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
        type ListType = Metadata<SmartModuleMetadataSpec>;

        fn convert_from<C: fluvio_controlplane_metadata::core::MetadataItem>(
            obj: &fluvio_controlplane_metadata::store::MetadataStoreObject<Self, C>,
        ) -> Self::ListType {
            Metadata {
                name: obj.key.to_string(),
                spec: obj.spec.to_metadata(),
                status: obj.status.clone(),
            }
        }
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
}
