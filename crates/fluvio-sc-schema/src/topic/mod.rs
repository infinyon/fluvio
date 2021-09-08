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
            assert_eq!(valid_topic_name("hello world"), false);
        }

        #[test]
        fn reject_topics_with_uppercase() {
            assert_eq!(valid_topic_name("helloWorld"), false);
        }

        #[test]
        fn reject_topics_with_underscore() {
            assert_eq!(valid_topic_name("hello_world"), false);
        }

        #[test]
        fn valid_topic() {
            assert!(valid_topic_name("hello-world"));
        }
        #[test]
        fn reject_topics_that_start_with_hyphen() {
            assert_eq!(valid_topic_name("-helloworld"), false);
        }
    }
}
mod convert {

    use std::convert::TryInto;
    use std::io::Error;
    use std::io::ErrorKind;

    use crate::objects::*;
    use super::*;

    impl From<TopicSpec> for AllCreatableSpec {
        fn from(spec: TopicSpec) -> Self {
            Self::Topic(spec)
        }
    }

    impl DeleteSpec for TopicSpec {
        fn into_request<K>(key: K) -> DeleteRequest
        where
            K: Into<Self::DeleteKey>,
        {
            DeleteRequest::Topic(key.into())
        }
    }

    impl ListSpec for TopicSpec {
        type Filter = NameFilter;

        fn into_list_request(filters: Vec<Self::Filter>) -> ListRequest {
            ListRequest::Topic(filters)
        }
    }

    impl TryInto<Vec<Metadata<TopicSpec>>> for ListResponse {
        type Error = Error;

        fn try_into(self) -> Result<Vec<Metadata<TopicSpec>>, Self::Error> {
            match self {
                ListResponse::Topic(s) => Ok(s),
                _ => Err(Error::new(ErrorKind::Other, "not spg")),
            }
        }
    }

    impl From<MetadataUpdate<TopicSpec>> for WatchResponse {
        fn from(update: MetadataUpdate<TopicSpec>) -> Self {
            Self::Topic(update)
        }
    }

    impl TryInto<MetadataUpdate<TopicSpec>> for WatchResponse {
        type Error = Error;

        fn try_into(self) -> Result<MetadataUpdate<TopicSpec>, Self::Error> {
            match self {
                WatchResponse::Topic(m) => Ok(m),
                _ => Err(Error::new(ErrorKind::Other, "not topic")),
            }
        }
    }
}
