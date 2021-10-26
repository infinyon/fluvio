pub use fluvio_controlplane_metadata::connector::*;

mod convert {

    use crate::{AdminSpec, NameFilter};
    use super::ManagedConnectorSpec;

    impl AdminSpec for ManagedConnectorSpec {
        type ListFilter = NameFilter;
        type ListType = Self;
        type WatchResponseType = Self;
        type DeleteKey = String;
    }
}
