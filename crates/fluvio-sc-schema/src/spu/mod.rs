pub use fluvio_controlplane_metadata::spu::*;

mod convert {

    use fluvio_controlplane_metadata::spu::CustomSpuKey;

    use crate::{AdminSpec, NameFilter};
    use super::{CustomSpuSpec, SpuSpec};

    impl AdminSpec for CustomSpuSpec {
        type ListFilter = NameFilter;
        type ListType = Self;

        type DeleteKey = CustomSpuKey;

        type WatchResponseType = Self;
    }

    impl AdminSpec for SpuSpec {
        type ListFilter = NameFilter;
        type ListType = Self;

        type DeleteKey = String;

        type WatchResponseType = Self;
    }
}
