pub use fluvio_controlplane_metadata::smartmodule::*;

mod convert {


    use crate::objects::{CreateType, Metadata};
    use crate::{AdminSpec,NameFilter};
    use super::SmartModuleSpec;

    impl AdminSpec for SmartModuleSpec {
        const AdminType: u8 = CreateType::SmartModule as u8;

        type ListFilter = NameFilter;


        type DeleteKey = String;

        type ListType = Metadata<Self>;
    }

    
}
