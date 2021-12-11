pub use fluvio_controlplane_metadata::partition::*;

mod convert {

    use crate::objects::ListRequest;
    use crate::objects::ListResponse;
    use crate::objects::ObjectFrom;
    use crate::objects::ObjectTryFrom;
    use crate::{
        AdminSpec, NameFilter,
        objects::{Metadata, WatchRequest, WatchResponse},
    };
    use super::*;

    impl AdminSpec for PartitionSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = Self;
        type ListType = Metadata<Self>;

        fn convert_from<C: fluvio_controlplane_metadata::core::MetadataItem>(
            obj: &fluvio_controlplane_metadata::store::MetadataStoreObject<Self, C>,
        ) -> Self::ListType {
            obj.clone().into()
        }
    }

    ObjectFrom!(WatchRequest, Partition);
    ObjectFrom!(WatchResponse, Partition);

    ObjectFrom!(ListRequest, Partition);
    ObjectFrom!(ListResponse, Partition);

    ObjectTryFrom!(WatchResponse, Partition);
    ObjectTryFrom!(ListResponse, Partition);
}
