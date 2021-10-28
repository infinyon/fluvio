pub use fluvio_controlplane_metadata::spu::*;

mod convert {

    use fluvio_controlplane_metadata::spu::CustomSpuKey;

    use crate::objects::ListRequest;
    use crate::objects::ListResponse;
    use crate::{
        AdminSpec, NameFilter,
        objects::{ObjectFrom, ObjectTryFrom, Metadata, WatchResponse, WatchRequest},
    };
    use super::{CustomSpuSpec, SpuSpec};

    impl AdminSpec for CustomSpuSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;

        type DeleteKey = CustomSpuKey;

        type WatchResponseType = Self;

        fn create_decoder() -> crate::CreateDecoder {
            crate::CreateDecoder::CustomSpu
        }
    }

    impl AdminSpec for SpuSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;

        type DeleteKey = String;

        type WatchResponseType = Self;

        fn create_decoder() -> crate::CreateDecoder {
            panic!("Spu cannot be created directly")
        }
    }

    ObjectFrom!(WatchRequest, Spu);
    ObjectFrom!(WatchResponse, Spu);

    ObjectFrom!(ListRequest, Spu);
    ObjectFrom!(ListResponse, Spu);

    ObjectTryFrom!(WatchResponse, Spu);
    ObjectTryFrom!(ListResponse, Spu);
}
