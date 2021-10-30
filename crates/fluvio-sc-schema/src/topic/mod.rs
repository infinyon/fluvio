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
    use crate::{AdminSpec, NameFilter};
    use crate::objects::{ObjectFrom, ObjectTryFrom, Metadata, WatchResponse, WatchRequest};

    use super::TopicSpec;

    impl AdminSpec for TopicSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = Self;
        type DeleteKey = String;
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

    use dataplane::api::{RequestHeader, RequestMessage, ResponseMessage};
    use dataplane::core::{Encoder, Decoder};
    use dataplane::api::Request;

    use crate::objects::{
        ListRequest, MetadataUpdate, ObjectApiListRequest, ObjectApiWatchRequest,
        ObjectApiWatchResponse, WatchResponse,
    };

    use super::*;

    fn create_req() -> ObjectApiListRequest {
        let list_request: ListRequest<TopicSpec> = ListRequest::new(vec![]);
        list_request.into()
    }

    fn create_res() -> ObjectApiWatchResponse {
        let update = MetadataUpdate {
            epoch: 2,
            changes: vec![],
            all: vec![],
        };
        let watch_response: WatchResponse<TopicSpec> = WatchResponse::new(update);
        watch_response.into()
    }

    #[test]
    fn test_from() {
        let req = create_req();

        assert!(matches!(req, ObjectApiListRequest::Topic(_)));
    }

    #[test]
    #[should_panic]
    // ObjectApi should not be able to decode directly, always thru middleware (ObjectDecoder or CreateDecoder)
    fn test_panic_decoding() {
        let req = create_req();

        let mut src = vec![];
        req.encode(&mut src, 0).expect("encoding");

        let _r = ObjectApiListRequest::decode_from(&mut Cursor::new(&src), 0).expect("decode");
    }

    #[test]
    fn test_encode_decoding() {
        use dataplane::api::Request;

        let req = create_req();

        let mut req_msg = RequestMessage::new_request(req);
        req_msg
            .get_mut_header()
            .set_client_id("test")
            .set_api_version(ObjectApiListRequest::API_KEY as i16);

        let mut src = vec![];
        req_msg.encode(&mut src, 0).expect("encoding");

        let dec_msg: RequestMessage<ObjectApiListRequest> = RequestMessage::decode_from(
            &mut Cursor::new(&src),
            ObjectApiListRequest::API_KEY as i16,
        )
        .expect("decode");
        assert!(matches!(dec_msg.request, ObjectApiListRequest::Topic(_)));
    }

    // test encoding and decoding of metadata update
    #[test]
    fn test_watch_response_encoding() {
        fluvio_future::subscriber::init_logger();
        let update = MetadataUpdate {
            epoch: 2,
            changes: vec![],
            all: vec![],
        };
        let watch_response: WatchResponse<TopicSpec> = WatchResponse::new(update);

        let mut src = vec![];
        watch_response
            .encode(&mut src, ObjectApiWatchRequest::API_KEY as i16)
            .expect("encoding");
        //watch_response.encode(&mut src, 0).expect("encoding");
        println!("output: {:#?}", src);
        let dec = WatchResponse::<TopicSpec>::decode_from(
            &mut Cursor::new(&src),
            ObjectApiWatchRequest::API_KEY as i16,
        )
        .expect("decode");
        assert_eq!(dec.inner().epoch, 2);
    }

    #[test]
    fn test_obj_watch_response_encode_decoding() {
        fluvio_future::subscriber::init_logger();

        let res = create_res();

        let mut header = RequestHeader::new(ObjectApiWatchRequest::API_KEY);
        header.set_client_id("test");
        header.set_correlation_id(11);
        let res_msg = ResponseMessage::from_header(&header, res);

        let mut src = vec![];
        res_msg
            .encode(&mut src, ObjectApiWatchRequest::API_KEY as i16)
            .expect("encoding");

        println!("output: {:#?}", src);

        assert_eq!(
            src.len(),
            res_msg.write_size(ObjectApiWatchRequest::API_KEY as i16)
        );

        let dec_msg: ResponseMessage<ObjectApiWatchResponse> = ResponseMessage::decode_from(
            &mut Cursor::new(&src),
            ObjectApiWatchRequest::API_KEY as i16,
        )
        .expect("decode");
        assert!(matches!(dec_msg.response, ObjectApiWatchResponse::Topic(_)));
    }
}
