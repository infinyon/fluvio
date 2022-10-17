pub use fluvio_controlplane_metadata::partition::*;

mod convert {

    use crate::objects::ListRequest;
    use crate::objects::ListResponse;
    use crate::objects::ObjectFrom;
    use crate::objects::ObjectTryFrom;
    use crate::{
        AdminSpec,
        objects::{WatchRequest, WatchResponse},
    };
    use super::*;

    impl AdminSpec for PartitionSpec {}

    ObjectFrom!(WatchRequest, Partition);
    ObjectFrom!(WatchResponse, Partition);

    ObjectFrom!(ListRequest, Partition);
    ObjectFrom!(ListResponse, Partition);

    ObjectTryFrom!(WatchResponse, Partition);
    ObjectTryFrom!(ListResponse, Partition);
}
