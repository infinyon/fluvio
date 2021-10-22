pub use fluvio_controlplane_metadata::partition::*;

mod convert {

    use crate::objects::CreateType;
    use crate::{AdminSpec,NameFilter};
    use super::*;

    impl AdminSpec for PartitionSpec {
        const AdminType: u8 = CreateType::Partition;

        type ListFilter = NameFilter;

        type DeleteKey = String;
    }

    
}
