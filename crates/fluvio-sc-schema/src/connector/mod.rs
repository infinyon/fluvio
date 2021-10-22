pub use fluvio_controlplane_metadata::connector::*;

mod convert {

    use crate::objects::{CreateType, MetadataUpdate};

    use crate::{AdminSpec, NameFilter};
    use crate::objects::Metadata;
    use super::ManagedConnectorSpec;

    impl AdminSpec for ManagedConnectorSpec {
        const AdminType: u8 = CreateType::ManagedConnector as u8;

        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = MetadataUpdate<Self>;
        type DeleteKey = String;
    }
}
