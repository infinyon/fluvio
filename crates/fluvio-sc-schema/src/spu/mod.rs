pub use fluvio_controlplane_metadata::spu::*;

mod convert {

    use fluvio_controlplane_metadata::spu::CustomSpuKey;

    use crate::{
        AdminSpec, NameFilter,
        objects::{Metadata, WatchResponse, ObjectApiWatchResponse},
    };
    use super::{CustomSpuSpec, SpuSpec};

    impl AdminSpec for CustomSpuSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;

        type DeleteKey = CustomSpuKey;

        type WatchResponseType = Self;
    }

    impl AdminSpec for SpuSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;

        type DeleteKey = String;

        type WatchResponseType = Self;
    }

    impl From<WatchResponse<SpuSpec>> for ObjectApiWatchResponse {
        fn from(response: WatchResponse<SpuSpec>) -> Self {
            ObjectApiWatchResponse::Spu(response)
        }
    }
}
