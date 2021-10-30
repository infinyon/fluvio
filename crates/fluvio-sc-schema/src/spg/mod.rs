pub use fluvio_controlplane_metadata::spg::*;

mod convert {

    use crate::{
        AdminSpec, NameFilter,
        objects::{
            CreateRequest, DeleteRequest, ListRequest, ListResponse, Metadata, ObjectFrom,
            ObjectTryFrom, WatchRequest, WatchResponse,
        },
    };
    use super::SpuGroupSpec;

    impl AdminSpec for SpuGroupSpec {
        type ListFilter = NameFilter;

        type DeleteKey = String;

        type ListType = Metadata<Self>;

        type WatchResponseType = Self;

        const CREATE_TYPE: u8 =2;

        fn create_decoder() -> crate::CreateDecoder {
            crate::CreateDecoder::SPG
        }
    }

    ObjectFrom!(CreateRequest, SpuGroup, Create);
    ObjectFrom!(WatchRequest, SpuGroup);
    ObjectFrom!(WatchResponse, SpuGroup);

    ObjectFrom!(ListRequest, SpuGroup);
    ObjectFrom!(ListResponse, SpuGroup);

    ObjectTryFrom!(WatchResponse, SpuGroup);
    ObjectTryFrom!(ListResponse, SpuGroup);

    ObjectFrom!(DeleteRequest, SpuGroup);
}
