#![allow(clippy::assign_op_pattern)]

use std::convert::TryInto;
use std::io::Error as IoError;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::io::AsyncRead;
use futures_util::io::AsyncWrite;

use fluvio_future::zero_copy::ZeroCopyWrite;
use fluvio_protocol::api::{
    api_decode, ApiMessage, Request, RequestHeader, RequestMessage, ResponseMessage,
};
use fluvio_protocol::bytes::Buf;
use fluvio_protocol::derive::Decode;
use fluvio_protocol::derive::Encode;

use fluvio_socket::FlvSocketError;
use fluvio_socket::InnerFlvSink;
use fluvio_socket::InnerFlvSocket;

use crate::api_loop;
use crate::call_service;
use crate::KfService;

#[fluvio(encode_discriminant)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub(crate) enum TestKafkaApiEnum {
    Echo = 1000,
    Save = 1001,
}

impl Default for TestKafkaApiEnum {
    fn default() -> TestKafkaApiEnum {
        TestKafkaApiEnum::Echo
    }
}

#[derive(Decode, Encode, Debug, Default)]
pub(crate) struct EchoRequest {
    msg: String,
}

impl EchoRequest {
    pub(crate) fn new(msg: String) -> Self {
        EchoRequest { msg }
    }
}

impl Request for EchoRequest {
    const API_KEY: u16 = TestKafkaApiEnum::Echo as u16;
    type Response = EchoResponse;
}

#[derive(Decode, Encode, Default, Debug)]
pub(crate) struct EchoResponse {
    pub msg: String,
}

#[derive(Decode, Encode, Debug, Default)]
pub(crate) struct SaveRequest {}
impl Request for SaveRequest {
    const API_KEY: u16 = TestKafkaApiEnum::Save as u16;
    type Response = SaveResponse;
}

#[derive(Decode, Encode, Debug, Default)]
pub(crate) struct SaveResponse {}

#[derive(Debug, Encode)]
pub(crate) enum TestApiRequest {
    EchoRequest(RequestMessage<EchoRequest>),
    SaveRequest(RequestMessage<SaveRequest>),
}

// Added to satisfy Encode/Decode traits
impl Default for TestApiRequest {
    fn default() -> TestApiRequest {
        TestApiRequest::EchoRequest(RequestMessage::default())
    }
}

impl ApiMessage for TestApiRequest {
    type ApiKey = TestKafkaApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        match header.api_key().try_into()? {
            TestKafkaApiEnum::Echo => api_decode!(TestApiRequest, EchoRequest, src, header),
            TestKafkaApiEnum::Save => api_decode!(TestApiRequest, SaveRequest, src, header),
        }
    }
}

#[derive(Debug)]
pub(crate) struct TestContext {}

impl TestContext {
    pub(crate) fn new() -> Self {
        TestContext {}
    }
}

pub(crate) type SharedTestContext = Arc<TestContext>;

#[derive(Debug)]
pub(crate) struct TestService {}

impl TestService {
    pub fn new() -> TestService {
        Self {}
    }
}

async fn handle_echo_request(
    msg: RequestMessage<EchoRequest>,
) -> Result<ResponseMessage<EchoResponse>, IoError> {
    let mut response = EchoResponse::default();
    response.msg = msg.request.msg.clone();
    Ok(msg.new_response(response))
}

#[async_trait]
impl<S> KfService<S> for TestService
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Context = SharedTestContext;
    type Request = TestApiRequest;

    async fn respond(
        self: Arc<Self>,
        _context: Self::Context,
        socket: InnerFlvSocket<S>,
    ) -> Result<(), FlvSocketError>
    where
        InnerFlvSink<S>: ZeroCopyWrite,
    {
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<TestApiRequest, TestKafkaApiEnum>();

        api_loop!(
            api_stream,
            TestApiRequest::EchoRequest(request) => call_service!(
                request,
                handle_echo_request(request),
                sink,
                "echo request handler"
            ),
            TestApiRequest::SaveRequest(_request) =>  {
                drop(api_stream);
                //let _orig_socket: FlvSocket  = (sink,stream).into();
                break;
            }
        );

        Ok(())
    }
}
