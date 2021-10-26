pub use fluvio_controlplane_metadata::table::*;

mod convert {

    use crate::{AdminSpec, NameFilter};
    use super::TableSpec;

    impl AdminSpec for TableSpec {
        type ListFilter = NameFilter;
        type ListType = Self;
        type WatchResponseType = Self;

        type DeleteKey = String;
    }
}
