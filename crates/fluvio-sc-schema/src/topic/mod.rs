pub use fluvio_controlplane_metadata::topic::*;

pub mod validate {
    use crate::shared::validate_resource_name;

    /// Ensure a topic can be created with a given name.
    /// Topics name can only be formed by lowercase alphanumeric elements and hyphens.
    /// They should start and finish with an alphanumeric character.
    pub fn valid_topic_name(name: &str) -> bool {
        validate_resource_name(name).is_ok()
    }
}

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
