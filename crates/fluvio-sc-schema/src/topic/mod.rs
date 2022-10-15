pub use fluvio_controlplane_metadata::topic::*;
pub mod validate {
    /// Ensure a topic can be created with a given name.
    /// Topics name can only be formed by lowercase alphanumeric elements and hyphens.
    /// They should start and finish with an alphanumeric character.
    pub fn valid_topic_name(name: &str) -> bool {
        name.chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
            && !name.ends_with('-')
            && !name.starts_with('-')
    }

    #[cfg(test)]
    mod tests {
        use crate::topic::validate::valid_topic_name;

        #[test]
        fn reject_topics_with_spaces() {
            assert!(!valid_topic_name("hello world"));
        }

        #[test]
        fn reject_topics_with_uppercase() {
            assert!(!valid_topic_name("helloWorld"));
        }

        #[test]
        fn reject_topics_with_underscore() {
            assert!(!valid_topic_name("hello_world"));
        }

        #[test]
        fn valid_topic() {
            assert!(valid_topic_name("hello-world"));
        }
        #[test]
        fn reject_topics_that_start_with_hyphen() {
            assert!(!valid_topic_name("-helloworld"));
        }
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
