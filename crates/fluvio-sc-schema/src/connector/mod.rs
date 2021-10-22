pub use fluvio_controlplane_metadata::connector::*;

mod convert {

 
    use crate::objects::CreateType;
    use crate::{AdminSpec,NameFilter};
    use super::*;


    impl AdminSpec for ManagedConnectorSpec {
        const AdminType: u8 = CreateType::MANAGED_CONNECTOR;

        type ListFilter = NameFilter;


        type DeleteKey = String;
    }

   
}
