#![allow(clippy::assign_op_pattern)]

use std::io::Error as IoError;
use std::convert::TryInto;

use kf_protocol::api::KfRequestMessage;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::RequestHeader;
use kf_protocol::api::api_decode;
use kf_protocol::bytes::Buf;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use kf_protocol::api::Request;

#[fluvio_kf(encode_discriminant)]
#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
#[repr(u16)]
pub enum TestKafkaApiEnum {
    Echo = 1000,
    Status = 1001,
}

impl Default for TestKafkaApiEnum {
    fn default() -> TestKafkaApiEnum {
        TestKafkaApiEnum::Echo
    }
}

#[derive(Decode, Encode, Debug, Default)]
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
#[derive(Decode, Encode, Debug, Default)]
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

#[derive(Decode, Encode, Debug, Default)]
pub struct AsyncStatusResponse {
    pub status: i32,
}

#[derive(Encode, Debug)]
pub enum TestApiRequest {
    EchoRequest(RequestMessage<EchoRequest>),
    AsyncStatusRequest(RequestMessage<AsyncStatusRequest>),
    Noop(bool),
}

// Added to satisfy Encode/Decode traits
impl Default for TestApiRequest {
    fn default() -> TestApiRequest {
        TestApiRequest::Noop(true)
    }
}

impl KfRequestMessage for TestApiRequest {
    type ApiKey = TestKafkaApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        tracing::debug!("decoding test api request: {:#?}", header);

        match header.api_key().try_into()? {
            TestKafkaApiEnum::Echo => api_decode!(TestApiRequest, EchoRequest, src, header),
            TestKafkaApiEnum::Status => {
                api_decode!(TestApiRequest, AsyncStatusRequest, src, header)
            }
        }
    }
}

#[derive(Decode, Encode, Default, Debug)]
pub struct EchoResponse {
    pub msg: String,
}

impl EchoResponse {
    pub fn new(msg: String) -> Self {
        Self { msg }
    }
}
