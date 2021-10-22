pub use fluvio_controlplane_metadata::connector::*;

mod convert {

    use crate::objects::CreateType;

    use crate::{AdminSpec, NameFilter};
    use crate::objects::Metadata;
    use super::ManagedConnectorSpec;

    impl AdminSpec for ManagedConnectorSpec {
        const AdminType: u8 = CreateType::MANAGED_CONNECTOR as u8;

        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;

        type DeleteKey = String;
    }
}
