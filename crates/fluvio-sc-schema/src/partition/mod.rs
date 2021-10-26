pub use fluvio_controlplane_metadata::partition::*;

mod convert {

    use crate::{AdminSpec, NameFilter, objects::Metadata};
    use super::*;

    impl AdminSpec for PartitionSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = Self;

        type DeleteKey = String;

        type ListType = Metadata<Self>;
    }
}
