pub use fluvio_controlplane_metadata::spg::*;

mod convert {


    use crate::objects::CreateType;
    use crate::{AdminSpec,NameFilter};
    use super::*;

    impl AdminSpec for SpuGroupSpec {
        const AdminType: u8 = CreateType::SpuGroup;

        type ListFilter = NameFilter;

        type DeleteKey = String;
    }

    
}
