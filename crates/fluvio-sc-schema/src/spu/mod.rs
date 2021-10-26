pub use fluvio_controlplane_metadata::spu::*;

mod convert {

    use fluvio_controlplane_metadata::spu::CustomSpuKey;

    use crate::objects::{Metadata, MetadataUpdate};
    use crate::{AdminSpec, NameFilter};
    use super::{CustomSpuSpec, SpuSpec};

    impl AdminSpec for CustomSpuSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = MetadataUpdate<Self>;

        type DeleteKey = CustomSpuKey;
    }

    impl AdminSpec for SpuSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = MetadataUpdate<Self>;
        type DeleteKey = String;
    }
}
