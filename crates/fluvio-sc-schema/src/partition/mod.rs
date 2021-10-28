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

        type DeleteKey = String;

        type ListType = Metadata<Self>;

        fn create_decoder() -> crate::CreateDecoder {
            panic!("Partition cannot be created directly")
        }
    }

    ObjectFrom!(WatchRequest, Partition);
    ObjectFrom!(WatchResponse, Partition);

    ObjectFrom!(ListRequest, Partition);
    ObjectFrom!(ListResponse, Partition);

    ObjectTryFrom!(WatchResponse, Partition);
    ObjectTryFrom!(ListResponse, Partition);
}
