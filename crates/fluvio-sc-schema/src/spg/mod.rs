pub use fluvio_controlplane_metadata::spg::*;

mod convert {

    use crate::objects::{CreateType, Metadata, MetadataUpdate};
    use crate::{AdminSpec, NameFilter};
    use super::SpuGroupSpec;

    impl AdminSpec for SpuGroupSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = MetadataUpdate<Self>;

        type DeleteKey = String;

        type ListType = Metadata<Self>;
    }
}
