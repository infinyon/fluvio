pub use fluvio_controlplane_metadata::smartstream::*;

mod convert {

    use crate::AdminSpec;
    use crate::CreatableAdminSpec;
    use crate::DeletableAdminSpec;
    use crate::NameFilter;
    use crate::objects::CreateFrom;
    use crate::objects::DeleteRequest;
    use crate::objects::ListRequest;
    use crate::objects::ListResponse;
    use crate::objects::Metadata;
    use crate::objects::ObjectFrom;
    use crate::objects::ObjectTryFrom;
    use crate::objects::WatchRequest;
    use crate::objects::WatchResponse;

    use super::SmartStreamSpec;

    impl AdminSpec for SmartStreamSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = Self;
        type ListType = Metadata<Self>;
    }

    impl CreatableAdminSpec for SmartStreamSpec {
        const CREATE_TYPE: u8 = 10;
    }

    impl DeletableAdminSpec for SmartStreamSpec {
        type DeleteKey = String;
    }

    CreateFrom!(SmartStreamSpec, SmartStream);
    ObjectFrom!(WatchRequest, SmartStream);
    ObjectFrom!(WatchResponse, SmartStream);
    ObjectFrom!(ListRequest, SmartStream);
    ObjectFrom!(ListResponse, SmartStream);
    ObjectFrom!(DeleteRequest, SmartStream);

    ObjectTryFrom!(WatchResponse, SmartStream);
    ObjectTryFrom!(ListResponse, SmartStream);
}
