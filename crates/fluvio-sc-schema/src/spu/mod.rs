pub use fluvio_controlplane_metadata::spu::*;

mod convert {


    use crate::objects::CreateType;
    use crate::{AdminSpec,NameFilter};
    use super::*;

    impl AdminSpec for CustomSpuSpec {
        const AdminType: u8 = CreateType::CustomSpu;

        type ListFilter = NameFilter;

        type DeleteKey = String;
    }

    impl AdminSpec for SpuSpec {
        const AdminType: u8 = CreateType::Spu;

        type ListFilter = NameFilter;

        type DeleteKey = String;

    }

    
}
