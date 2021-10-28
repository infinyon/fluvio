pub use fluvio_controlplane_metadata::smartmodule::*;

mod convert {

    use crate::{
        AdminSpec, NameFilter,
        objects::{
            ListRequest, ListResponse, Metadata, ObjectApiWatchResponse, ObjectFrom, ObjectTryFrom,
            WatchRequest, WatchResponse,
        },
    };
    use super::SmartModuleSpec;

    impl AdminSpec for SmartModuleSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = Self;

        type DeleteKey = String;

        type ListType = Metadata<Self>;

        fn create_decoder() -> crate::CreateDecoder {
            crate::CreateDecoder::SmartModule
        }
    }

    ObjectFrom!(WatchRequest, SmartModule);
    ObjectFrom!(WatchResponse, SmartModule);

    ObjectFrom!(ListRequest, SmartModule);
    ObjectFrom!(ListResponse, SmartModule);

    ObjectTryFrom!(WatchResponse, SmartModule);
    ObjectTryFrom!(ListResponse, SmartModule);
}
