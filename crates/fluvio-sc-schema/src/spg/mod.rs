pub use fluvio_controlplane_metadata::spg::*;

mod convert {

    use crate::{AdminSpec, NameFilter, objects::Metadata};
    use super::SpuGroupSpec;

    impl AdminSpec for SpuGroupSpec {
        type ListFilter = NameFilter;

        type DeleteKey = String;

        type ListType = Metadata<Self>;

        type WatchResponseType = Self;
    }
}
