#![allow(clippy::assign_op_pattern)]

use std::convert::TryInto;
use std::io::Error as IoError;

use tracing::{debug};

use fluvio_protocol::api::{api_decode, ApiMessage, Request, RequestHeader, RequestMessage};
use fluvio_protocol::bytes::Buf;
use fluvio_protocol::derive::{Decoder, Encoder};

#[repr(u16)]
#[derive(Encoder, Decoder, Eq, PartialEq, Debug, Clone, Copy)]
#[fluvio(encode_discriminant)]
#[derive(Default)]
pub enum TestKafkaApiEnum {
    #[default]
    Echo = 1000,
    Status = 1001,
}

#[derive(Decoder, Encoder, Debug, Default)]
pub struct EchoRequest {
    pub msg: String,
}

impl EchoRequest {
    pub fn new(msg: String) -> Self {
        Self { msg }
    }
}

impl Request for EchoRequest {
    const API_KEY: u16 = TestKafkaApiEnum::Echo as u16;
    type Response = EchoResponse;
}

/// request to send back status
#[derive(Decoder, Encoder, Debug, Default)]
pub struct AsyncStatusRequest {
    pub count: i32,
}

impl AsyncStatusRequest {
    pub fn new(count: i32) -> Self {
        Self { count }
    }
}

impl Request for AsyncStatusRequest {
    const API_KEY: u16 = TestKafkaApiEnum::Status as u16;
    type Response = AsyncStatusResponse;
}

#[derive(Decoder, Encoder, Debug, Default)]
pub struct AsyncStatusResponse {
    pub status: i32,
}

#[derive(Encoder, Debug)]
pub enum TestApiRequest {
    #[fluvio(tag = 0)]
    EchoRequest(RequestMessage<EchoRequest>),
    #[fluvio(tag = 1)]
    AsyncStatusRequest(RequestMessage<AsyncStatusRequest>),
    #[fluvio(tag = 2)]
    Noop(bool),
}

// Added to satisfy Encoder/Decoder traits
impl Default for TestApiRequest {
    fn default() -> TestApiRequest {
        TestApiRequest::Noop(true)
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
        debug!("decoding test api request: {:#?}", header);

        match header.api_key().try_into()? {
            TestKafkaApiEnum::Echo => api_decode!(TestApiRequest, EchoRequest, src, header),
            TestKafkaApiEnum::Status => {
                api_decode!(TestApiRequest, AsyncStatusRequest, src, header)
            }
        }
    }
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct EchoResponse {
    pub msg: String,
}

impl EchoResponse {
    pub fn new(msg: String) -> Self {
        Self { msg }
    }
}
