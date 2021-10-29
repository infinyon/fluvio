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

    use crate::objects::CreateRequest;
    use crate::objects::DeleteRequest;
    use crate::objects::ListRequest;
    use crate::objects::ListResponse;
    use crate::{AdminSpec, CreateDecoder, NameFilter};
    use crate::objects::{ObjectFrom, ObjectTryFrom, Metadata, WatchResponse, WatchRequest};

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

    ObjectFrom!(CreateRequest, Topic, Create);
    ObjectFrom!(WatchRequest, Topic);
    ObjectFrom!(WatchResponse, Topic);
    ObjectFrom!(ListRequest, Topic);
    ObjectFrom!(ListResponse, Topic);
    ObjectFrom!(DeleteRequest, Topic);

    ObjectTryFrom!(WatchResponse, Topic);
    ObjectTryFrom!(ListResponse, Topic);
}

#[cfg(test)]
mod test {

    use std::io::Cursor;

    use dataplane::api::RequestMessage;
    use dataplane::core::{Encoder, Decoder};

    use crate::objects::{ListRequest, ObjectApiListRequest};
    use crate::ObjectDecoder;
    use super::*;

    fn create_req() -> (ObjectApiListRequest, ObjectDecoder) {
        let list_request: ListRequest<TopicSpec> = ListRequest::new(vec![]);
        list_request.into()
    }

    #[test]
    fn test_from() {
        let (req, mw) = create_req();

        assert!(matches!(req, ObjectApiListRequest::Topic(_)));
        assert_eq!(mw, ObjectDecoder::new::<TopicSpec>());
    }

    #[test]
    #[should_panic]
    // ObjectApi should not be able to decode directly, always thru middleware (ObjectDecoder or CreateDecoder)
    fn test_panic_decoding() {
        let (req, _mw) = create_req();

        let mut src = vec![];
        req.encode(&mut src, 0).expect("encoding");

        let _r = ObjectApiListRequest::decode_from(&mut Cursor::new(&src), 0).expect("decode");
    }

    #[test]
    fn test_encode_decoding() {
        use dataplane::api::Request;

        let (req, mw) = create_req();

        let mut req_msg = RequestMessage::request_with_mw(req, mw);
        req_msg
            .get_mut_header()
            .set_client_id("test")
            .set_api_version(ObjectApiListRequest::API_KEY as i16);

        let mut src = vec![];
        req_msg.encode(&mut src, 0).expect("encoding");

        let dec_msg: RequestMessage<ObjectApiListRequest, ObjectDecoder> =
            RequestMessage::decode_from(
                &mut Cursor::new(&src),
                ObjectApiListRequest::API_KEY as i16,
            )
            .expect("decode");
        assert!(matches!(dec_msg.request, ObjectApiListRequest::Topic(_)));
    }
}
