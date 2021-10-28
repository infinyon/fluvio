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

    use crate::{AdminSpec, CreateDecoder, NameFilter};
    use crate::objects::{Metadata, ObjectApiWatchResponse, WatchResponse};

    use super::TopicSpec;

    impl AdminSpec for TopicSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = Self;
        type DeleteKey = String;

        fn create_decoder() -> crate::CreateDecoder {
            CreateDecoder::TOPIC
        }
    }

    impl From<WatchResponse<TopicSpec>> for ObjectApiWatchResponse {
        fn from(response: WatchResponse<TopicSpec>) -> Self {
            ObjectApiWatchResponse::Topic(response)
        }
    }
}
