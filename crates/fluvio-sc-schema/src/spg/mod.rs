pub use fluvio_controlplane_metadata::spg::*;

mod convert {

    use crate::{
        AdminSpec, CreatableAdminSpec, DeletableAdminSpec, NameFilter,
        objects::{
            CreateRequest, DeleteRequest, ListRequest, ListResponse, Metadata, ObjectFrom,
            ObjectTryFrom, WatchRequest, WatchResponse,
        },
    };
    use super::SpuGroupSpec;

    impl AdminSpec for SpuGroupSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = Self;
    }

    impl CreatableAdminSpec for SpuGroupSpec {
        const CREATE_TYPE: u8 = 2;
    }

    impl DeletableAdminSpec for SpuGroupSpec {
        type DeleteKey = String;
    }

    ObjectFrom!(CreateRequest, SpuGroup);
    ObjectFrom!(WatchRequest, SpuGroup);
    ObjectFrom!(WatchResponse, SpuGroup);

    ObjectFrom!(ListRequest, SpuGroup);
    ObjectFrom!(ListResponse, SpuGroup);

    ObjectTryFrom!(WatchResponse, SpuGroup);
    ObjectTryFrom!(ListResponse, SpuGroup);

    ObjectFrom!(DeleteRequest, SpuGroup);
}
