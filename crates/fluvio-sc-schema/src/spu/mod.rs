pub use fluvio_controlplane_metadata::spu::*;

mod convert {


    use crate::objects::{CreateType, Metadata};
    use crate::{AdminSpec,NameFilter};
    use super::{CustomSpuSpec,SpuSpec};

    impl AdminSpec for CustomSpuSpec {
        const AdminType: u8 = CreateType::CustomSPU as u8;

        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;

        type DeleteKey = String;
    }

    impl AdminSpec for SpuSpec {

        // not used
        const AdminType: u8 = 0;

        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;

        type DeleteKey = String;

    }

    
}
