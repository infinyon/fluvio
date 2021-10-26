pub use fluvio_controlplane_metadata::smartmodule::*;

mod convert {

    use crate::{AdminSpec, NameFilter, objects::Metadata};
    use super::SmartModuleSpec;

    impl AdminSpec for SmartModuleSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = Self;

        type DeleteKey = String;

        type ListType = Metadata<Self>;
    }
}
