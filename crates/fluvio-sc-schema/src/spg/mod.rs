pub use fluvio_controlplane_metadata::spg::*;

mod convert {

    use crate::objects::{Metadata};
    use crate::{AdminSpec, NameFilter};
    use super::SpuGroupSpec;

    impl AdminSpec for SpuGroupSpec {
        type ListFilter = NameFilter;

        type DeleteKey = String;

        type ListType = Self;

        type WatchResponseType = Self;
    }
}
