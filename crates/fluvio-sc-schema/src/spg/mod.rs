pub use fluvio_controlplane_metadata::spg::*;

mod convert {


    use crate::objects::{CreateType, Metadata};
    use crate::{AdminSpec,NameFilter};
    use super::SpuGroupSpec;

    impl AdminSpec for SpuGroupSpec {
        const AdminType: u8 = CreateType::SPG as u8;

        type ListFilter = NameFilter;

        type DeleteKey = String;

        type ListType = Metadata<Self>;
    }

    
}
