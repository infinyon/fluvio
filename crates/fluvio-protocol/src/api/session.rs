use std::marker::PhantomData;
use super::Request;
use crate::Encoder;
use crate::Decoder;

pub const CLOSE_SESSION_API_KEY: u16 = 10_000;

#[derive(Debug, Encoder, Decoder, Default)]
pub struct CloseSessionRequest {
    pub session_id: i32,
}

impl Request for CloseSessionRequest {
    const API_KEY: u16 = CLOSE_SESSION_API_KEY;
    type Response = CloseSessionResponse;
}

pub type CloseSessionResponse = PhantomData<bool>;
