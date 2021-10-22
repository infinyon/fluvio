pub use fluvio_controlplane_metadata::partition::*;

mod convert {

    use crate::objects::{Metadata, MetadataUpdate};
    use crate::{AdminSpec, NameFilter};
    use super::*;

    impl AdminSpec for PartitionSpec {
        // not used
        const AdminType: u8 = 0;

        type ListFilter = NameFilter;
        type WatchResponseType = MetadataUpdate<Self>;

        type DeleteKey = String;

        type ListType = Metadata<Self>;
    }
}
