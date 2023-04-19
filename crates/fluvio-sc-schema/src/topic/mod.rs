pub use fluvio_controlplane_metadata::topic::*;

mod convert {
    use crate::CreatableAdminSpec;
    use crate::DeletableAdminSpec;
    use crate::objects::CreateFrom;
    use crate::objects::DeleteRequest;
    use crate::objects::ListRequest;
    use crate::objects::ListResponse;
    use crate::{AdminSpec};
    use crate::objects::{ObjectFrom, ObjectTryFrom, WatchResponse, WatchRequest};

    use super::TopicSpec;

    impl AdminSpec for TopicSpec {}

    impl CreatableAdminSpec for TopicSpec {
        const CREATE_TYPE: u8 = 0;
    }

    impl DeletableAdminSpec for TopicSpec {
        type DeleteKey = String;
    }

    CreateFrom!(TopicSpec, Topic);
    ObjectFrom!(WatchRequest, Topic);
    ObjectFrom!(WatchResponse, Topic);
    ObjectFrom!(ListRequest, Topic);
    ObjectFrom!(ListResponse, Topic);
    ObjectFrom!(DeleteRequest, Topic);

    ObjectTryFrom!(WatchResponse, Topic);
    ObjectTryFrom!(ListResponse, Topic);
}
