use std::io::Error as IoError;
use std::path::Path;
use std::fmt;
use std::fmt::Display;

use tracing::trace;
use bytes::Buf;
use bytes::BufMut;

use crate::Decoder;
use crate::Encoder;
use crate::Version;

use super::Request;
use super::RequestHeader;
use super::response::ResponseMessage;

/// Start of API request
#[derive(Debug)]
pub struct RequestMessage<R> {
    pub header: RequestHeader,
    pub request: R,
}

impl<R> RequestMessage<R> {
    #[allow(unused)]
    pub fn get_mut_header(&mut self) -> &mut RequestHeader {
        &mut self.header
    }
}

impl<R> fmt::Display for RequestMessage<R>
where
    R: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.header, self.request)
    }
}

impl<R> Default for RequestMessage<R>
where
    R: Request + Default,
{
    fn default() -> Self {
        let mut header = RequestHeader::default();
        header.set_api_version(R::DEFAULT_API_VERSION);

        Self {
            header,
            request: R::default(),
        }
    }
}

impl<R> RequestMessage<R>
where
    R: Request,
{
    /// create with header, this assume header is constructed from higher request
    /// no api key check is performed since it is already done
    #[allow(unused)]
    pub fn new(header: RequestHeader, request: R) -> Self {
        Self { header, request }
    }

    /// create from request, header is implicitly created from key in the request
    #[allow(unused)]
    pub fn new_request(request: R) -> Self {
        let mut header = RequestHeader::new(R::API_KEY);
        header.set_api_version(R::DEFAULT_API_VERSION);

        Self { header, request }
    }

    #[allow(unused)]
    pub fn get_header_request(self) -> (RequestHeader, R) {
        (self.header, self.request)
    }

    #[allow(unused)]
    #[allow(unused)]
    pub fn request(&self) -> &R {
        &self.request
    }

    #[allow(unused)]
    pub fn new_response(&self, response: R::Response) -> ResponseMessage<R::Response> {
        Self::response_with_header(&self.header, response)
    }

    pub fn response_with_header<H>(header: H, response: R::Response) -> ResponseMessage<R::Response>
    where
        H: Into<i32>,
    {
        ResponseMessage::new(header.into(), response)
    }

    #[allow(unused)]
    pub fn decode_response<T>(
        &self,
        src: &mut T,
        version: Version,
    ) -> Result<ResponseMessage<R::Response>, IoError>
    where
        T: Buf,
    {
        ResponseMessage::decode_from(src, version)
    }

    #[allow(unused)]
    pub fn decode_response_from_file<H: AsRef<Path>>(
        &self,
        file_name: H,
        version: Version,
    ) -> Result<ResponseMessage<R::Response>, IoError> {
        ResponseMessage::decode_from_file(file_name, version)
    }

    /// helper function to set client id
    #[allow(unused)]
    pub fn set_client_id<T>(mut self, client_id: T) -> Self
    where
        T: Into<String>,
    {
        self.header.set_client_id(client_id);
        self
    }
}

impl<R> Decoder for RequestMessage<R>
where
    R: Request,
{
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), IoError>
    where
        T: Buf,
    {
        self.header.decode(src, version)?;
        self.request.decode(src, self.header.api_version())?;
        Ok(())
    }
}

impl<R> Encoder for RequestMessage<R>
where
    R: Request,
{
    fn write_size(&self, version: Version) -> usize {
        self.header.write_size(version) + self.request.write_size(self.header.api_version())
    }

    fn encode<T>(&self, out: &mut T, version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        let len = self.write_size(version);
        trace!(
            "encoding kf request: {} version: {}, len: {}",
            std::any::type_name::<R>(),
            version,
            len
        );

        trace!("encoding request header: {:#?}", &self.header);
        self.header.encode(out, version)?;

        trace!("encoding request: {:#?}", &self.request);
        self.request.encode(out, self.header.api_version())?;
        Ok(())
    }
}

impl<R: Clone> Clone for RequestMessage<R> {
    fn clone(&self) -> Self {
        Self {
            header: self.header.clone(),
            request: self.request.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::io::Cursor;
    use std::io::Error as IoError;
    use std::convert::TryInto;
    use bytes::{Buf, BufMut};
    use crate::api::ApiMessage;

    #[repr(u16)]
    #[derive(Eq, PartialEq, Debug, Clone, Copy, Encoder, Decoder)]
    #[fluvio(encode_discriminant)]
    #[derive(Default)]
    pub enum TestApiKey {
        #[default]
        ApiVersion = 0,
    }

    #[derive(Decoder, Encoder, Debug, Default)]
    pub struct ApiVersionRequest {}

    impl Request for ApiVersionRequest {
        const API_KEY: u16 = TestApiKey::ApiVersion as u16;

        type Response = ApiVersionResponse;
    }

    #[derive(Encoder, Decoder, Default, Debug)]
    pub struct ApiVersionResponse {
        pub error_code: i16,
        pub api_versions: Vec<ApiVersion>,
        pub throttle_time_ms: i32,
    }

    #[derive(Encoder, Decoder, Default, Debug)]
    pub struct ApiVersion {
        pub api_key: i16,
        pub min_version: i16,
        pub max_version: i16,
    }

    #[repr(u16)]
    #[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
    #[fluvio(encode_discriminant)]
    #[derive(Default)]
    pub enum TestApiEnum {
        #[default]
        ApiVersion = 18,
    }

    #[test]
    fn test_decode_header() -> Result<(), IoError> {
        // API versions request
        // API key: API Versions (18)
        // API version: 1
        // correlation id: 1,
        // string length 10
        // client id: consumer-1
        let data = [
            0x00, 0x12, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0a, 0x63, 0x6f, 0x6e, 0x73,
            0x75, 0x6d, 0x65, 0x72, 0x2d, 0x31,
        ];

        let header: RequestHeader = RequestHeader::decode_from(&mut Cursor::new(&data), 0)?;

        assert_eq!(header.api_key(), TestApiEnum::ApiVersion as u16);
        assert_eq!(header.api_version(), 1);
        assert_eq!(header.correlation_id(), 1);
        assert_eq!(header.client_id(), "consumer-1");

        Ok(())
    }

    #[test]
    fn test_encode_header() {
        let req_header = RequestHeader::new_with_client(
            TestApiEnum::ApiVersion as u16,
            String::from("consumer-1"),
        );
        let expected_result = [
            0x00, 0x12, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0a, 0x63, 0x6f, 0x6e, 0x73,
            0x75, 0x6d, 0x65, 0x72, 0x2d, 0x31,
        ];

        let mut result = vec![];
        let req_result = req_header.encode(&mut result, 0);

        assert!(req_result.is_ok());
        assert_eq!(result, expected_result);
    }

    pub enum TestApiRequest {
        ApiVersionRequest(RequestMessage<ApiVersionRequest>),
    }

    impl Default for TestApiRequest {
        fn default() -> TestApiRequest {
            TestApiRequest::ApiVersionRequest(RequestMessage::<ApiVersionRequest>::default())
        }
    }

    impl ApiMessage for TestApiRequest {
        type ApiKey = TestApiEnum;

        fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
        where
            Self: Default + Sized,
            Self::ApiKey: Sized,
            T: Buf,
        {
            match header.api_key().try_into()? {
                TestApiEnum::ApiVersion => {
                    let request = ApiVersionRequest::decode_from(src, header.api_version())?;
                    Ok(TestApiRequest::ApiVersionRequest(RequestMessage::new(
                        header, request,
                    )))
                }
            }
        }
    }

    impl Encoder for TestApiRequest {
        fn write_size(&self, version: Version) -> usize {
            match self {
                TestApiRequest::ApiVersionRequest(response) => response.write_size(version),
            }
        }

        fn encode<T>(&self, src: &mut T, version: Version) -> Result<(), IoError>
        where
            T: BufMut,
        {
            match self {
                TestApiRequest::ApiVersionRequest(response) => {
                    response.encode(src, version)?;
                }
            }
            Ok(())
        }
    }

    #[test]
    fn test_encode_message() {
        let mut message = RequestMessage::new_request(ApiVersionRequest {});
        message
            .get_mut_header()
            .set_client_id("consumer-1".to_owned())
            .set_correlation_id(5);

        let mut out = vec![];
        message.encode(&mut out, 0).expect("encode work");
        let mut encode_bytes = Cursor::new(&out);

        let res_msg_result: Result<RequestMessage<ApiVersionRequest>, IoError> =
            Decoder::decode_from(&mut encode_bytes, 0);

        let msg = res_msg_result.unwrap();
        assert_eq!(msg.header.correlation_id(), 5);
    }
}
