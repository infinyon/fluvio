pub use fluvio_controlplane_metadata::spg::*;

mod convert {

    use crate::{
        AdminSpec, CreatableAdminSpec, DeletableAdminSpec,
        objects::{
            CreateFrom, DeleteRequest, ListRequest, ListResponse, ObjectFrom, ObjectTryFrom,
            WatchRequest, WatchResponse,
        },
    };
    use super::SpuGroupSpec;

    impl AdminSpec for SpuGroupSpec {}

    impl CreatableAdminSpec for SpuGroupSpec {
        const CREATE_TYPE: u8 = 2;
    }

    impl DeletableAdminSpec for SpuGroupSpec {
        type DeleteKey = String;
    }

    CreateFrom!(SpuGroupSpec, SpuGroup);
    ObjectFrom!(WatchRequest, SpuGroup);
    ObjectFrom!(WatchResponse, SpuGroup);

    ObjectFrom!(ListRequest, SpuGroup);
    ObjectFrom!(ListResponse, SpuGroup);

    ObjectTryFrom!(WatchResponse, SpuGroup);
    ObjectTryFrom!(ListResponse, SpuGroup);

    ObjectFrom!(DeleteRequest, SpuGroup);
}
