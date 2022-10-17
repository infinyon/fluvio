pub use fluvio_controlplane_metadata::derivedstream::*;

mod convert {

    use crate::AdminSpec;
    use crate::CreatableAdminSpec;
    use crate::DeletableAdminSpec;
    use crate::objects::CreateFrom;
    use crate::objects::DeleteRequest;
    use crate::objects::ListRequest;
    use crate::objects::ListResponse;
    use crate::objects::ObjectFrom;
    use crate::objects::ObjectTryFrom;
    use crate::objects::WatchRequest;
    use crate::objects::WatchResponse;

    use super::DerivedStreamSpec;

    impl AdminSpec for DerivedStreamSpec {}

    impl CreatableAdminSpec for DerivedStreamSpec {
        const CREATE_TYPE: u8 = 10;
    }

    impl DeletableAdminSpec for DerivedStreamSpec {
        type DeleteKey = String;
    }

    CreateFrom!(DerivedStreamSpec, DerivedStream);
    ObjectFrom!(WatchRequest, DerivedStream);
    ObjectFrom!(WatchResponse, DerivedStream);
    ObjectFrom!(ListRequest, DerivedStream);
    ObjectFrom!(ListResponse, DerivedStream);
    ObjectFrom!(DeleteRequest, DerivedStream);

    ObjectTryFrom!(WatchResponse, DerivedStream);
    ObjectTryFrom!(ListResponse, DerivedStream);
}
