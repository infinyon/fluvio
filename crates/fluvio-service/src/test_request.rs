#![allow(clippy::assign_op_pattern)]

use std::convert::TryInto;
use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use anyhow::Result;

use fluvio_protocol::api::{
    api_decode, ApiMessage, Request, RequestHeader, RequestMessage, ResponseMessage,
};
use fluvio_protocol::bytes::Buf;
use fluvio_protocol::derive::Decoder;
use fluvio_protocol::derive::Encoder;

use fluvio_socket::FluvioSocket;

use crate::api_loop;
use crate::call_service;
use crate::{FluvioService, ConnectInfo};

#[repr(u16)]
#[derive(PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
#[derive(Default)]
pub(crate) enum TestKafkaApiEnum {
    #[default]
    Echo = 1000,
    Save = 1001,
}

#[derive(Decoder, Encoder, Debug, Default)]
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

#[derive(Decoder, Encoder, Default, Debug)]
pub(crate) struct EchoResponse {
    pub msg: String,
}

#[derive(Decoder, Encoder, Debug, Default)]
pub(crate) struct SaveRequest {}
impl Request for SaveRequest {
    const API_KEY: u16 = TestKafkaApiEnum::Save as u16;
    type Response = SaveResponse;
}

#[derive(Decoder, Encoder, Debug, Default)]
pub(crate) struct SaveResponse {}

#[derive(Debug, Encoder)]
pub(crate) enum TestApiRequest {
    #[fluvio(tag = 0)]
    EchoRequest(RequestMessage<EchoRequest>),
    #[fluvio(tag = 1)]
    SaveRequest(RequestMessage<SaveRequest>),
}

// Added to satisfy Encoder/Decoder traits
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
pub(crate) struct TestService {
    pub(crate) processed_requests: AtomicUsize,
}

impl TestService {
    pub fn new() -> TestService {
        Self {
            processed_requests: Default::default(),
        }
    }
}

async fn handle_echo_request(
    msg: RequestMessage<EchoRequest>,
) -> Result<ResponseMessage<EchoResponse>, IoError> {
    let response = EchoResponse {
        msg: msg.request.msg.clone(),
    };
    Ok(msg.new_response(response))
}

#[async_trait]
impl FluvioService for TestService {
    type Request = TestApiRequest;
    type Context = SharedTestContext;

    async fn respond(
        mut self: Arc<Self>,
        _context: Self::Context,
        socket: FluvioSocket,
        _connection: ConnectInfo,
    ) -> Result<()> {
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<TestApiRequest, TestKafkaApiEnum>();

        api_loop!(
            api_stream,
            TestApiRequest::EchoRequest(request) => {
                call_service!(
                request,
                handle_echo_request(request),
                sink,
                "echo request handler"
                );
                self.processed_requests.fetch_add(1, Ordering::SeqCst);
            },
            TestApiRequest::SaveRequest(_request) =>  {
                drop(api_stream);
                self.processed_requests.fetch_add(1, Ordering::SeqCst);
                //let _orig_socket: FlvSocket  = (sink,stream).into();
                break;
            }
        );

        Ok(())
    }
}
