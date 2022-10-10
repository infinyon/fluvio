pub use fluvio_controlplane_metadata::smartmodule::*;
pub use convert::SmartModuleFilter;

mod convert {

    use fluvio_protocol::{Encoder, Decoder};
    use crate::{
        AdminSpec, CreatableAdminSpec, DeletableAdminSpec,
        objects::{
            CreateFrom, DeleteRequest, ListRequest, ListResponse, Metadata, ObjectFrom,
            ObjectTryFrom, WatchRequest, WatchResponse,
        },
    };
    use super::SmartModuleSpec;

    impl AdminSpec for SmartModuleSpec {
        type ListFilter = SmartModuleFilter;
        type WatchResponseType = Self;
        type ListType = Metadata<Self>;
    }

    impl CreatableAdminSpec for SmartModuleSpec {
        const CREATE_TYPE: u8 = 4;
    }

    impl DeletableAdminSpec for SmartModuleSpec {
        type DeleteKey = String;
    }

    #[derive(Debug, Encoder, Decoder, Default)]
    pub struct SmartModuleFilter {
        pub name: String,
        #[fluvio(min_version = crate::objects::MIN_API_WITH_FILTER)]
        pub summary: bool       // if true, only return summary
    }

    impl From<String> for SmartModuleFilter {
        fn from(name: String) -> Self {
            Self {
                name,
                summary: false
            }
        }
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
