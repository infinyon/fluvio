#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use dataplane::bytes::Buf;
use dataplane::api::{api_decode, ApiMessage, Request, RequestHeader, RequestMessage};
use dataplane::derive::{Encode, Decode};

pub type AuthorizationScopes = Vec<String>;

pub const AUTH_REQUEST_API_KEY: u16 = 8;

// Auth Test Request & Response
#[derive(Decode, Encode, Debug, Default)]
pub struct AuthRequest {
    pub principal: String,
    pub scopes: AuthorizationScopes,
}

impl AuthRequest {
    pub fn new(principal: String, scopes: AuthorizationScopes) -> Self {
        AuthRequest { principal, scopes }
    }
}

impl Request for AuthRequest {
    const API_KEY: u16 = AUTH_REQUEST_API_KEY;
    type Response = AuthResponse;
}

#[derive(Decode, Encode, Default, Debug)]
pub struct AuthResponse {
    pub success: bool,
}

#[derive(Debug)]
pub enum AuthorizationApiRequest {
    AuthRequest(RequestMessage<AuthRequest>),
}

// Added to satisfy Encode/Decode traits
impl Default for AuthorizationApiRequest {
    fn default() -> AuthorizationApiRequest {
        AuthorizationApiRequest::AuthRequest(RequestMessage::default())
    }
}

impl ApiMessage for AuthorizationApiRequest {
    type ApiKey = u16;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, std::io::Error>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        match header.api_key() {
            AUTH_REQUEST_API_KEY => api_decode!(AuthorizationApiRequest, AuthRequest, src, header),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "api auth header key should be set to {:?}",
                    AUTH_REQUEST_API_KEY
                ),
            )),
        }
    }
}
