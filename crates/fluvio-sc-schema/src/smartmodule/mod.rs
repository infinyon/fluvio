pub use fluvio_controlplane_metadata::smartmodule::*;

mod convert {


    use crate::objects::CreateType;
    use crate::{AdminSpec,NameFilter};
    use super::*;

    impl AdminSpec for SmartModuleSpec {
        const AdminType: u8 = CreateType::SmartModule;

        type ListFilter = NameFilter;


        type DeleteKey = String;
    }

    
}
