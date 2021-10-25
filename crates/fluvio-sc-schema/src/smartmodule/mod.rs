pub use fluvio_controlplane_metadata::smartmodule::*;

mod convert {

    use crate::objects::{CreateType, Metadata, MetadataUpdate};
    use crate::{AdminSpec, NameFilter};
    use super::SmartModuleSpec;

    impl AdminSpec for SmartModuleSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = MetadataUpdate<Self>;

        type DeleteKey = String;

        type ListType = Metadata<Self>;
    }
}
