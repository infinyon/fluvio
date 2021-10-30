pub use fluvio_controlplane_metadata::smartmodule::*;

mod convert {

    use crate::{
        AdminSpec, NameFilter,
        objects::{
            CreateRequest, DeleteRequest, ListRequest, ListResponse, Metadata, ObjectFrom,
            ObjectTryFrom, WatchRequest, WatchResponse,
        },
    };
    use super::SmartModuleSpec;

    impl AdminSpec for SmartModuleSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = Self;

        type DeleteKey = String;
        const CREATE_TYPE: u8 = 4;

        type ListType = Metadata<Self>;
    }

    ObjectFrom!(CreateRequest, SmartModule, Create);
    ObjectFrom!(WatchRequest, SmartModule);
    ObjectFrom!(WatchResponse, SmartModule);
    ObjectFrom!(ListRequest, SmartModule);
    ObjectFrom!(ListResponse, SmartModule);
    ObjectFrom!(DeleteRequest, SmartModule);

    ObjectTryFrom!(WatchResponse, SmartModule);
    ObjectTryFrom!(ListResponse, SmartModule);
}
