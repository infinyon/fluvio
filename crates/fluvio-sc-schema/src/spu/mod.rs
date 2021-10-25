pub use fluvio_controlplane_metadata::spu::*;

mod convert {

    use crate::objects::{CreateType, Metadata, MetadataUpdate};
    use crate::{AdminSpec, NameFilter};
    use super::{CustomSpuSpec, SpuSpec};

    impl AdminSpec for CustomSpuSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = MetadataUpdate<Self>;

        type DeleteKey = String;
    }

    impl AdminSpec for SpuSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = MetadataUpdate<Self>;
        type DeleteKey = String;
    }
}
