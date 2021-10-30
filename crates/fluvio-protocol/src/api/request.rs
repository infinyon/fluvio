use std::io::Error as IoError;
use std::path::Path;
use std::fmt;
use std::fmt::{Display, Debug};

use tracing::trace;
use bytes::Buf;
use bytes::BufMut;

use crate::Decoder;
use crate::Encoder;
use crate::Version;

use super::{Request};
use super::RequestHeader;
use super::response::ResponseMessage;

pub use middleware::*;

/// Start of API request
#[derive(Debug)]
pub struct RequestMessage<R, M = DefaultRequestMiddleWare> {
    pub header: RequestHeader,
    pub middleware: M,
    pub request: R,
}

impl<R, M> RequestMessage<R, M> {
    #[allow(unused)]
    pub fn get_mut_header(&mut self) -> &mut RequestHeader {
        &mut self.header
    }
}

impl<R, M> fmt::Display for RequestMessage<R, M>
where
    R: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.header, self.request)
    }
}

impl<R, M> Default for RequestMessage<R, M>
where
    R: Request<M> + Default,
    M: RequestMiddleWare,
{
    fn default() -> Self {
        let mut header = RequestHeader::default();
        header.set_api_version(R::DEFAULT_API_VERSION);

        Self {
            header,
            middleware: M::default(),
            request: R::default(),
        }
    }
}

impl<R> RequestMessage<R, DefaultRequestMiddleWare>
where
    R: Request<DefaultRequestMiddleWare>,
{
    /// create with header, this assume header is constructed from higher request
    /// no api key check is performed since it is already done
    #[allow(unused)]
    pub fn new(header: RequestHeader, request: R) -> Self {
        Self {
            header,
            middleware: DefaultRequestMiddleWare::default(),
            request,
        }
    }

    /// create from request, header is implicilty created from key in the request
    pub fn new_request(request: R) -> Self {
        Self {
            header: Self::create_header(),
            middleware: DefaultRequestMiddleWare::default(),
            request,
        }
    }

    pub fn response_with_header<H>(header: H, response: R::Response) -> ResponseMessage<R::Response>
    where
        H: Into<i32>,
    {
        ResponseMessage::new(header.into(), response)
    }

    #[allow(unused)]
    pub fn new_response(&self, response: R::Response) -> ResponseMessage<R::Response> {
        Self::response_with_header(&self.header, response)
    }
}

impl<R, M> RequestMessage<R, M> {
    /// create with header, this assume header is constructed from higher request
    /// no api key check is performed since it is already done
    pub fn new_with_mw(header: RequestHeader, middleware: M, request: R) -> Self {
        Self {
            header,
            middleware,
            request,
        }
    }

    /// destruct to header and request
    pub fn get_header_request(self) -> (RequestHeader, R) {
        (self.header, self.request)
    }

    /// destruct to header, middleware, and reqq
    pub fn get_header_request_middleware(self) -> (RequestHeader, R, M) {
        (self.header, self.request, self.middleware)
    }

    #[allow(unused)]
    #[allow(unused)]
    pub fn request(&self) -> &R {
        &self.request
    }
}

impl<R, M> RequestMessage<R, M>
where
    R: Request<M>,
    M: RequestMiddleWare,
{
    pub fn create_header() -> RequestHeader {
        let mut header = RequestHeader::new(R::API_KEY);
        header.set_api_version(R::DEFAULT_API_VERSION);
        header
    }

    /// create request with middleware
    pub fn request_with_mw(request: R, middleware: M) -> Self {
        Self {
            header: Self::create_header(),
            middleware,
            request,
        }
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

impl<R, M> RequestMessage<R, M>
where
    R: Request<M>,
    M: RequestMiddleWare,
{
    /// decode with already decoded header
    pub fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        T: Buf,
        Self: Default,
    {
        let version = header.api_version();
        let mut req = Self {
            header,
            ..Default::default()
        };

        req.middleware.decode(src, version)?;
        req.request
            .decode_with_middleware(src, &req.middleware, version)?;
        Ok(req)
    }
}

impl<R, M> Decoder for RequestMessage<R, M>
where
    R: Request<M>,
    M: RequestMiddleWare,
{
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), IoError>
    where
        T: Buf,
    {
        self.header.decode(src, version)?;
        self.middleware.decode(src, version)?;
        self.request
            .decode_with_middleware(src, &self.middleware, self.header.api_version())?;
        Ok(())
    }
}

impl<R, M> Encoder for RequestMessage<R, M>
where
    R: Request<M>,
    M: RequestMiddleWare,
{
    fn write_size(&self, version: Version) -> usize {
        self.header.write_size(version)
            + self.middleware.write_size(self.header.api_version())
            + self.request.write_size(self.header.api_version())
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

        trace!("encoding middleware: {:#?}", &self.middleware);
        self.middleware.encode(out, self.header.api_version())?;

        trace!("encoding request: {:#?}", &self.request);
        self.request.encode(out, self.header.api_version())?;
        Ok(())
    }
}

mod middleware {

    use super::*;

    pub trait RequestMiddleWare: Default + Encoder + Decoder + Debug {}

    /// Default Request request Middleware which does nothing.
    #[derive(Default, Debug)]
    pub struct DefaultRequestMiddleWare {}

    impl RequestMiddleWare for DefaultRequestMiddleWare {}

    impl Decoder for DefaultRequestMiddleWare {
        fn decode<T>(&mut self, _src: &mut T, _version: Version) -> Result<(), IoError>
        where
            T: Buf,
        {
            Ok(())
        }
    }

    impl Encoder for DefaultRequestMiddleWare {
        fn write_size(&self, _version: Version) -> usize {
            0
        }

        fn encode<T>(&self, _out: &mut T, _version: Version) -> Result<(), IoError>
        where
            T: BufMut,
        {
            Ok(())
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
    #[derive(PartialEq, Debug, Clone, Copy, Encoder, Decoder)]
    #[fluvio(encode_discriminant)]
    pub enum TestApiKey {
        ApiVersion = 0,
    }

    impl Default for TestApiKey {
        fn default() -> TestApiKey {
            TestApiKey::ApiVersion
        }
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
    #[derive(PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
    #[fluvio(encode_discriminant)]
    pub enum TestApiEnum {
        ApiVersion = 18,
    }

    impl Default for TestApiEnum {
        fn default() -> TestApiEnum {
            TestApiEnum::ApiVersion
        }
    }

    #[test]
    fn test_decode_header() -> Result<(), IoError> {
        // API versions request
        // API key: API Versions (18)
        // API version: 1
        // correlation id: 1,
        // strng length 10
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
